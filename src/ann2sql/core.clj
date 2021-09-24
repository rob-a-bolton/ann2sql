(ns ann2sql.core
  (:require [cheshire.core :as cheshire]
            [cli-matic.core :refer [run-cmd]]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]))

(def con (atom nil))

(defn json-k->kw
  [k]
  (-> k
      (.replaceAll "_" "-")
      .toLowerCase
      keyword))

(def cols 
  [[:hadm-id :int]
   [:row-id :int]
   [:id :int]
   [:tui :text]
   [:cui :text]
   [:source-value :text]
   [:status :text :null]
   [:m-start :int]
   [:m-end :int]
   [:acc :float]])

(defn ann->map
  [{:keys [id tui cui start end acc source-value meta-anns]}]
  [(Integer/valueOf id) tui cui source-value
   (Integer/valueOf start)
   (Integer/valueOf end)
   (Double/valueOf acc)
   (get-in meta-anns [:status :value])])

(defn record->map
  [record]
  (let [{:keys [hadm-id row-id annotations]} record]
    (map #(concat [(Integer/valueOf hadm-id) (Integer/valueOf row-id)]
                 (ann->map %))
         annotations)))

(defn file->record-map
  [file]
  (with-open [i (io/reader file)]
    (record->map (cheshire/parse-stream i json-k->kw))))

(defn get-record-rows
  [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (map file->record-map)
       (apply concat)
       (partition-all 1000)))

(defn import-records
  [{:keys [dir jdbc]}]
  (with-open [db-con (jdbc/get-connection {:jdbcUrl jdbc} {:auto-commit false})]
    (reset! con db-con)
    (jdbc/execute!
     db-con
     (sql/format {:create-table [:annotations :if-not-exists]
                  :with-columns cols}))
    (.commit db-con)
    (let [stmts (get-record-rows dir)]
      (doseq [batch stmts]
        (with-open [p (jdbc/prepare db-con ["INSERT INTO annotations (hadm_id, row_id, id, tui, cui, source_value, m_start, m_end, acc, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"])]
          (doseq [row batch]
            (.setInt p 1 (nth row 0)) ;hadm
            (.setInt p 2 (nth row 1)) ;row
            (.setInt p 3 (nth row 2)) ;id
            (.setString p 4 (nth row 3)) ;tui
            (.setString p 5 (nth row 4)) ;cui
            (.setString p 6 (nth row 5)) ;source
            (.setInt p 7 (nth row 6)) ;m_start
            (.setInt p 8 (nth row 7)) ;m_end
            (.setFloat p 9 (nth row 8)) ;acc
            (.setString p 10 (nth row 9)) ;status
            (.executeUpdate p)))
        (.commit db-con)))))

(def cli-configuration
  {:command "ann2sql"
   :description "Import annotation JSON records into a database"
   :version "1.0.0"
   :opts [{:option "jdbc"
           :default :present
           :type :string
           :as "JDBC connection string"}]
   :subcommands [{:command "import"
                  :description "Imports the JSON records"
                  :opts [{:as "Dir to scan for records"
                          :default "."
                          :type :string
                          :option "dir"}]
                  :runs import-records}]})

(defn -main
  [& args]
  (run-cmd args cli-configuration))
