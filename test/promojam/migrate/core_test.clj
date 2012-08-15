(ns promojam.migrate.core-test
  (:use [clojure.test]
        [promojam.migrate.core]
        [clojure.string :only (split)])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clj-time.format :as tf] )
    (:import java.io.File))


;; define the files in test/migrations
(def first-file "201208220190809_first.cql")
(def second-file "201208220190855_second.cql")
(def non-cql-file "not-cql.txt")
(def ks "testmigrations")
(def ks-helper "testhelpermigrations")




;; test helper functions

(deftest test-getting-new-migration-file-name
  (let [name "some test name" 
        no-name-migration (migrate-name)
        name-migration (migrate-name name)
        split-full-name  (-> (split name-migration #"\.") first (split #"\_"))]
        
    (is (= org.joda.time.DateTime (type (tf/parse migrate-formatter no-name-migration))))
    (is (= "cql" (last (split name-migration #"\."))))
    (is (= 4 (count split-full-name)))
    (is (= (= (nth split-full-name 1) "some")
           (= (nth split-full-name 2) "test")
           (= (nth split-full-name 3) "name")))))


(deftest get-migration-files 
  (let [files (sort (get-migrations (File. "./test/migrations/")))]
    (is (= (first files) first-file))
    (is (= (second files) second-file))
    (is (= -1 (.indexOf files non-cql-file)))))


 
(deftest test-calling-cql-source-with-valid-cql
  (let [ result ( cql-source "./test/cql/valid.cql")]
    (is (= 0 (:exit result)))
    (is (empty? (:out result)))
    (is (empty? (:err result)))))

(deftest test-calling-cql-source-with-invalid-cql-throws-execption
  (is (thrown? java.lang.Exception (cql-source "./test/cql/invalid.cql"))))





;; test core functions
(cc/connect! "127.0.0.1" "system")


(deftest initialize-the-migration-table 
  (let [first-run (initialize "127.0.0.1" ks)
       second-run (initialize "127.0.0.1" ks)]
    (is (cql/void-result? first-run))
    (is (empty? (:rows first-run)))
    (is (nil? second-run))))
      
  
;; this only tests that two files were executed 
;; not that they contain valid cql
(deftest run-two-sequential-migration-files
  (let [ result (migrate "127.0.0.1" ks "./test/migrations/") ]
    (println result)
    (is (= 2  ( count result)))))


(deftest run-migration-with-no-new-migrations 
  (let [history (sort (get-history "127.0.0.1" ks)) 
        result (migrate "127.0.0.1" ks "./test/migrations/" ) ]
    (is (= 2 (count history)))
    (is (= (first history) first-file))
    (is (= (second history) second-file))
    (is (empty? result))
    (cql/execute "USE system")
    (cql/execute "DROP KEYSPACE ? " [ks])))


                         
    
        


    


    
