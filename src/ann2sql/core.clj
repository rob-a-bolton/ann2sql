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

;; (s/def :rec/record)

(defn ann->map
  [{:keys [id tui cui start end acc source-value meta-anns]}]
  {:id id :tui tui :cui cui :source-value source-value
   :start (Integer/valueOf start)
   :end (Integer/valueOf end)
   :acc (Double/valueOf acc)
   :status (get-in meta-anns [:status :value])})

(defn record->map
  [record]
  ;; {:pre [s/valid? :rec/record record]}
  (let [{:keys [hadm-id row-id annotations]} record]
    ;; (doall (map #(println (format "[%s] %s: %s" hadm-id row-id (:cui %)))
    ;;             annotations))
    (map #(merge {:hadm-id hadm-id :row-id row-id}
                 (ann->map %))
         annotations)))

(defn file->record-map
  [file]
  (with-open [i (io/reader file)]
    (record->map (cheshire/parse-stream i json-k->kw))))

(defn get-record-stmts
  [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (map file->record-map)
       flatten
       (partition-all 1000)
       (map (fn [anns]
              (sql/format {:insert-into :annotations :values anns}
                          {:pretty true})))))

(defn prepare-bulk-stmt
  [table data]
  (sql/format 
   {:insert-into table
    :values (list (first data))}))

(defn import-records
  [{:keys [dir jdbc]}]
  (with-open [db-con (jdbc/get-connection {:jdbcUrl jdbc})]
    (reset! con db-con)
    (jdbc/execute!
     db-con
     (sql/format {:create-table [:annotations :if-not-exists]
                  :with-columns
                  [[:hadm-id :int]
                   [:row-id :int]
                   [:id :int]
                   [:tui :text]
                   [:cui :text]
                   [:source-value :text]
                   [:status :text :null]
                   [:start :int]
                   [:end :int]
                   [:acc :float]]}))
    (let [stmts (get-record-stmts dir)]
      (doseq [batch stmts]
        (jdbc/execute! db-con batch)))))

(def cli-configuration
  {:command "ann2sql"
   :description "Import annotation JSON records into a database"
   :version "0.0.1"
  ;;  :opts [{:option "dir"
  ;;          :default "."
  ;;          :type :string
  ;;          :as "The dir to search for JSON records"}]
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
  (run-cmd args cli-configuration)
  (println "f"))