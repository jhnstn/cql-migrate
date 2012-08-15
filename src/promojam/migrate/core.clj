(ns promojam.migrate.core
  (:use [clojure.set]
        [clojure.java.shell :only [sh]]
        [clojure.string :only (split)]
        [clojure.contrib.trace :only (dotrace)])
  (:import java.io.File)
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojure.contrib.string :as str]
            [clj-time.core :as tm] 
            [clj-time.format :as tf] 
            [clojure.contrib.string :as str] ))

(def migrations-dir "./migrations/")
(def migrate-formatter ( tf/formatter "YYYYMMDDHHMMSS"))
(def timestamp-formatter (tf/formatter "yyyy-mm-dd HH:mm:ssZ"))


(defn migrate-name 
  "return formated name for migration file"
   ([] 
   (tf/unparse migrate-formatter (tm/now)))

  ([name]
   (str (tf/unparse migrate-formatter (tm/now))
        "_" 
        (.replaceAll name " " "_" )
        ".cql")))

(defn get-migrations
  "get a lazy sequence of files in a directory"
  [d]
 (remove false? (for [f (.listFiles d)]
    (and  (.isFile f) (= "cql" (-> (.getName f) (split #"\.") last ))
      (.getName f)))))
   
 
(defn cql-source
  "Mimic cqlsh's source command"
  [f]
  (let [file (str "--file=" f) 
        result (sh "cqlsh" "--cql3" file)
        err  (get result :err)
        ]
    (try
      (if (not-empty err )
        (throw (Exception. err))))
    result ))


(defn get-history
  "selects migrate history"
  ([] (get-history "127.0.0.1" "migrations"))
  ([interface ks]
    (cc/connect! interface ks)
    (let [ res (cql/execute-raw "SELECT file FROM history") 
           col (-> res :rows)
           ]
      (set ( for [c col] (-> (second c) second first (get :value )))))))


;; TODO creates a new migration file with basic info
(defn new-migration 
  "create a new migration file in ./migrations directory"
  [name]
  name)


(defn migrate 
  "scan migrations directory and update database"
  ([] (migrate "127.0.0.1" "migrations" "./migrations/" ))
  ([interface ks dir]
    (let [ ran (get-history interface ks)
           files (set ( sort (get-migrations (File. dir))))
           new-files (difference (difference files #{false}) ran)     
          ]
      (println ks)
      (if (empty? new-files) (prn "No new migrations found")
		      (doseq [ f new-files ]
		        (let [file (str dir f)]
			        (try
			          (cc/connect! interface ks)
		            (cql-source file)
			          (cql/insert "history" {:action "update"
			                                 :file f
			                                 :migrated_on (tf/unparse timestamp-formatter (tm/now))
			                                 :success "true"}{})
		            (println "Finished migrating " f )
			          (catch Exception e ( println "Unable to run " f )
			                             ( println e ))))))
          new-files )))
		     
      
	                             
	 
(defn initialize 
  "set up keyspace for storing migration status"
  ([] (initialize "127.0.0.1" "migrations"))
  ([interface ks]
  (try 
         (cc/connect! interface "system")
         (cql/execute "CREATE KEYSPACE ? WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;"  [ks])
         (cql/execute "USE ? " [ks])
         (cql/execute  "CREATE COLUMNFAMILY history ( 
                         action varchar , file varchar , migrated_on timestamp , success varchar ,
                         PRIMARY KEY (action , file , migrated_on));")
    ( catch Exception e (prn "Migrations already intitialized" )
                        (prn e)))))
  

(defn -main
  "Run migration "
  [& args]
   (migrate))

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   