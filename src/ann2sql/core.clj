(ns ann2sql.core
  (:require [cheshire.core :as cheshire]
            [cli-matic.core :refer [run-cmd]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as result-set])
  (:gen-class))

(defn float-type
  "Try to guess the best column type for the database.
   Fallback on :float when unknown."
  [jdbc-str]
  (let [db-type (-> jdbc-str (str/split #":") second str/lower-case)]
    (case db-type
      "sqlite" :real
      "mariadb" :double
      "postgresql" (keyword "double precision")
      :float)))

(defn mk-medcat-cols
  "Create the column spec for annotation columns."
  [jdbc-str]
  [[:seq_id :int]
   [:cui :text]
   [:source_value :text]
   [:m_start :int]
   [:m_end :int]
   [:acc (float-type jdbc-str)]])

(defn annotation->map
  "Pull only the necessary keys from the annotation."
  [{seq-id :id cui :cui source-value :source_value 
    m-start :start m-end :end acc :acc}]
  {:seq_id seq-id
   :cui cui
   :source_value source-value
   :m_start m-start
   :m_end m-end
   :acc acc})

(defn get-single-row
  "Fetch a single row from a table. Useful to probe for column types."
  [db-con table cols]
  (first
   (jdbc/execute!
    db-con
    (-> (apply h/select cols)
        (h/from table)
        (h/limit 1)
        (sql/format))
    {:builder-fn (result-set/as-maps-adapter
                  result-set/as-unqualified-maps
                  result-set/clob-column-reader)})))

(defn field->col-type
  "Map a java class to a column spec data type."
  [jdbc-str field]
  (condp = (type field)
    java.lang.Integer :int
    java.lang.Double (float-type jdbc-str)
    java.lang.Float :float
    java.util.Date :date
    :text))

(defn row->col-defs
  "Given a row, estimate the column spec needed to re-create the table."
  [jdbc-str row]
  (map (fn [[k v]] [k (field->col-type jdbc-str v)]) row))

(defn probe-table
  "Given a table and list of cols to check, try to find what column spec
   is needed to represent these in the destination database."
  [jdbc-str db-con table cols]
  (row->col-defs jdbc-str (get-single-row db-con table cols)))

(defn clean-bulk-results
  [results]
  (if (string? (first results))
    (map #(cheshire.core/parse-string % true) results)
    results))

(defn process-bulk
  "Given a MedCATservice process_bulk endpoint URL and sequence of documents,
   annotate the documents and return the raw annotation maps."
  [url texts]
  (-> url
      (client/post {:body (cheshire/generate-string
                           {:content (map (fn [text] {:text text}) texts)})
                    :content-type :json
                    :as :json})
      (get-in [:body :result])
      clean-bulk-results))

(defn get-bulk-annotations
  "Given the MedCATservice bulk endpoint URL and sequence of documents,
   annotate them and return a sequence of annotation maps containing only
   the keys/vals we would like to store."
  [medcat-url texts]
  (as-> texts $
    (process-bulk medcat-url $)
    (map (comp #(map annotation->map %) vals :annotations) $)))

;; TODO: Remove the println and replace with better logging/feedback
(defn annotate-batch
  "Annotate a batch of rows and recombine the annotations with the unique
   fields specified from the row they were annotated from."
  [medcat-url cols col]
  (fn [rows]
    (println (str "Annotating batch of " (count rows) " rows"))
    (let [texts (map col rows)
          meta-maps (map #(select-keys % cols) rows)
          annotation-batches (get-bulk-annotations medcat-url texts)]
      (mapcat (fn [meta annotations] (map #(merge meta %) annotations))
              meta-maps annotation-batches))))

(defn store-batch
  "Insert batch of rows into table."
  [con table]
  (fn [rows]
    (jdbc/execute!
     con
     (-> (h/insert-into table)
         (h/values rows)
         (sql/format)))
    (.commit con)))

(defn get-data-rows
  "Query a connection for the specified columns in the specified table and
   return a lazy iterator."
  [con table cols]
  (jdbc/execute!
   con
   (-> (apply h/select cols)
       (h/from table)
       (sql/format))
   {:builder-fn (result-set/as-maps-adapter
                 result-set/as-unqualified-maps
                 result-set/clob-column-reader)}))

(defn do-all
  "Fold a collection with this to get a bool representing whether all values
   were truthy (true), or some value was falsey (false)."
  ([] true)
  ([x] (some? x))
  ([x y] (and (some? x) (some? y))))

(defn import-data
  [{:keys [src-jdbc dst-jdbc src-table dst-table
           src-columns text-column src-batch-size dst-batch-size
           drop-tables create-tables medcat-url]}]
  (with-open [con-in (jdbc/get-connection {:jdbcUrl src-jdbc} {:auto-commit false})
              con-out (jdbc/get-connection {:jdbcUrl dst-jdbc} {:auto-commit false})]
    (when drop-tables
      (jdbc/execute!
       con-out
       (sql/format {:drop-table [:if-exists dst-table]})))
    (when create-tables
      (let [unique-cols (probe-table dst-jdbc con-in src-table src-columns)]
        (jdbc/execute!
         con-out
         (sql/format {:create-table [dst-table :if-not-exists]
                      :with-columns (concat unique-cols (mk-medcat-cols dst-jdbc))}))))
    (.commit con-out)
    (transduce
     (comp
      ;; partition incoming rows
      (partition-all src-batch-size)
      ;; annotate batch via process_bulk
      (mapcat (annotate-batch medcat-url src-columns text-column))
      ;; partition annotations into batches
      (partition-all dst-batch-size)
      ;; store batches
      (map (store-batch con-out dst-table)))
     do-all ;; Reduce results to single bool indicating if any inserts failed
     (get-data-rows con-in src-table (cons text-column src-columns)))))

(defn print-error
  "Print one or more strings to stderr."
  [& msgs]
  (binding [*out* *err*]
    (apply println msgs)))

(defn print-jdbc-help
  "Prints some examples of viable jdbc connection strings."
  []
  (print-error
   (str/join
    (System/lineSeparator)
    ["A JDBC connection string is a database-driver specific URL used to configure the connection."
     "Here are some common examples:"
     " jdbc:sqlite:test.db"
     " jdbc:sqlite:C:/SomeFolder/My.db"
     " jdbc:mariadb://localhost:3306/MyDBname?user=maria&password=hunter2"
     " jdbc:postgresql://localhost/n2c2?user=n2c2&password=badpassword"
     ""
     "The only drivers bundled with this app are sqlite, mariadb, and postgresql."])))

(defn try-import-data
  "Wrap the import function with an error handler to provide some friendlier
   feedback to the user about common errors."
  [args]
  (try
    (import-data args)
    (catch java.sql.SQLException e
      (print-error (.getMessage e))
      (cond
        (str/includes? (.getMessage e) "No suitable driver found")
        (print-jdbc-help)
        (re-find #"duplicate key value" (.getMessage e))
        (print-error "Duplicate data created. Please do not re-append the same patients/documents into a database.")
        (re-find #"(relation.*does not exist)|(no such table)" (.getMessage e))
        (print-error "Table not found. Check src table exists and either dst table exists or --create-tables option specified.")
        :else
        (.printStackTrace e))
      (System/exit 1))))

(def cli-configuration
  {:command "ann2sql"
   :description "Import annotation JSON records into a database"
   :version "1.0.0"
   :opts [{:option "medcat-url"
           :short "u"
           :default "http://127.0.0.1:5000/api/process_bulk"
           :type :string
           :as "URL to process documents with MedCATservice"}
          {:option "src-jdbc"
           :short "s"
           :env "SRC_JDBC"
           :default :present
           :type :string
           :as "Input database JDBC connection string"}
          {:option "dst-jdbc"
           :short "d"
           :env "DST_JDBC"
           :default :present
           :type :string
           :as "Output database JDBC connection string"}
          {:option "src-table"
           :short "S"
           :default :present
           :type :keyword
           :as "Table in source database containing data to annotate"}
          {:option "dst-table"
           :short "D"
           :default :present
           :type :keyword
           :as "Table in destination database to output annotations to"}
          {:option "src-columns"
           :short "c"
           :multiple true
           :type :keyword
           :as "Src columns to preserve, use option multiple times for multiple cols"}
          {:option "text-column"
           :short "t"
           :default :present
           :type :keyword
           :as "Column in source containing input documents to be annotated"}
          {:option "src-batch-size"
           :short "b"
           :default 100
           :type :int
           :as "How many documents to submit at once for batch processing to MedCATservice"}
          {:option "dst-batch-size"
           :short "B"
           :default 1000
           :type :int
           :as "How many annotations to batch up to bulk write to the destination database"}
          {:option "drop-tables"
           :default false
           :type :with-flag
           :as "Whether to drop the output table before beginning."}
          {:option "create-tables"
           :default false
           :type :with-flag
           :as "Whether to create the output table before beginning (if it doesn't exist already)"}]
   :runs try-import-data})

(defn -main
  [& args]
  (run-cmd args cli-configuration))
