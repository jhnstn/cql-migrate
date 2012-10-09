(ns promojam.migrate.core
  (:use [promojam.migrate.cqlsh-dsl]
        [clojure.set]
        [clojure.java.shell :only [sh]]
        [clojure.string :only (split)]
        [clojure.contrib.trace :only (dotrace)]
        [clojure.tools.cli :only (cli)])
  (:import java.io.File)
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojure.contrib.string :as str]
            [clj-time.core :as tm]
            [clj-time.format :as tf]
            [clojure.contrib.string :as str] )
  (:gen-class))



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
 (remove nil? (for [f (.listFiles d)]
    (and  (.isFile f) (re-find #"[cql|clj]$"  (.getName f) )
      (.getName f)))))


(defn cql-source
  "Mimic cqlsh's source command"
  [host f]
  (let [file (str "--file=" f)
        result  (sh "cqlsh" "--cql3" host  file)
        err  (get result :err)
        ]
    (try
      (if (not-empty err )
        (throw (Exception. err))))
    result ))


(defn get-history
  "selects migrate history"
  [host ks]
    (cc/connect! host ks)
    (let [ res (cql/execute-raw "SELECT file FROM history")
           col (-> res :rows)
           ]
      (set ( for [c col] (-> (second c) second first (get :value ))))))


;; TODO creates a new migration file with basic info
(defn new-migration
  "create a new migration file in ./migrations directory"
  [name]
  name)


(defn migrate
  "scan migrations directory and update database"
 [args]

    (let [ host (:host args)
           ks   (:keyspace args)
           dir  (:dir args)
           ran (get-history host ks)
           verbose (:verbose args)
           files (set ( sort (get-migrations (File. dir))))
           new-files (difference (difference files #{false}) ran)
          ]
      (if (empty? new-files) (prn "No new migrations found")

        (do
          (if verbose (println "Running migrations at host " host))
          (doseq [ f new-files ]
            (let [file (str dir f)]
              (try
                (if (re-find #"[.cql]$" f)
                  (cql-source host file)
                  (doseq [q (flatten (load-file file))]

                      (if verbose (println q))
                      (cql/execute q)))
                (cc/connect! host  ks)
                (cql/insert "history" {:action "update"
			                                 :file f
			                                 :migrated_on (tf/unparse timestamp-formatter (tm/now))
			                                 :success "true"}{})
		            (if verbose (println "Finished migrating " f ))
			          (catch Exception e ( println "Unable to run " f )
			                             ( println e )))))))))




(defn initialize
  "Set up keyspace for storing migration status"
  [args]
  (let [host (:host args)
        ks   (:keyspace args)
        verbose (:verbose args)]
    (try
      (if verbose (println "Initializing history"))
      (cc/connect! host "system")
      (cql/execute "CREATE KEYSPACE ? WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;"  [ks])
      (cql/execute "USE ? " [ks])
      (cql/execute  "CREATE COLUMNFAMILY history (
                         action varchar , file varchar , migrated_on timestamp , success varchar ,
                         PRIMARY KEY (action , file , migrated_on));")
      (if verbose (println "History keyspace" ks "has been created at host" host ))
      (catch Exception e (println "Initialization Error:" )
        (prn e)))))


(defn -main
  "Run migration"
  [& args]
  (let [[opts com banner] (cli args
                                ["-h" "--host" "Cassandra host" :default "localhost"]
                                ["-d" "--dir"  "Path to migration file directory" :default "./"]
                                ["-k" "--keyspace" "Cassandra migrations keyspace" :default "migrations"]
                                ["-v" "--[no-]verbose" :default true]
                                ["--help" "Show help" :flag true  :default false])
        command (or (first com) "migrate")]


   (when (:help opts)
     (println "CQL MIGRATION")
     (println)
     (println "Options:")
     (println " [-h][-d] migrate - runs the migrations in -d at host -h ")
     (println " [-h][-k] init    - intializes the migration history at host -h in keyspace -k")
     (println " [-h][-k] history - fetch the migration history at host -h in keyspace -k")
     (println)
     (println banner)
     (System/exit 0))
   (case command
     "migrate" (do (migrate opts) (System/exit 0))
     ("initialize" "init")(do (initialize opts) (System/exit 0))
     "history" (let [history (get-history (:host opts) (:keyspace opts))]
                 (println "Migration history")
                 (println "file:")
                 (doseq [f history]
                   (println "\t" f))
                 (System/exit 0))

     "default" (println command " is not an option. use --help for more info on usage"))))
