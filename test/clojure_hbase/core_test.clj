(ns clojure-hbase.core-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
        clojure.stacktrace
        [clojure-hbase.core])
  (:require [clojure-hbase.admin :as adm])
  (:import [org.apache.hadoop.hbase HBaseTestingUtility]
           [org.apache.hadoop.hbase.util Bytes]
           [java.util UUID]))

;; HBaseTestingUtility instance

(def ^:dynamic *test-util* (atom nil))

(defn- setup-cluster [^HBaseTestingUtility tu]
  (.startMiniCluster tu 1)
  (set-config (.getConfiguration tu))
  (adm/set-admin-config (.getConfiguration tu))
  tu)

(defn once-start []
  (swap! *test-util*
         #(or % (setup-cluster (HBaseTestingUtility.)))))

(defn once-stop []
  (swap! *test-util* #(.shutdownMiniCluster %)))

(defn once-fixture [f]
  (once-start)
  (try
    (f)
    (finally
      (once-stop))))

(use-fixtures :once once-fixture)

(defn create-table
  "Create a table with families, return table name.
  A dummy column is added in case no families specified."
  [table-name & families]
  (->> (cons "_0" families)
       (map adm/column-descriptor)
       (interleave (repeat :family))
       (apply adm/table-descriptor table-name)
       (adm/create-table))
  table-name)

(defn drop-table [table-name]
  (adm/disable-table table-name)
  (adm/delete-table table-name))

(defmacro with-create-table [binding body]
  `(let [~(first binding) ~(second binding)]
     (try
       ~body
       (finally
         (drop-table ~(first binding))))))

(defn keywordize [x] (keyword (Bytes/toString x)))

(defn result->vector
  [result & {:as opts}]
  (as-vector result
             :map-family keywordize :map-qualifier keywordize
             :map-timestamp (:timestamp-fn opts identity)
             :map-value (:value-fn opts #(Bytes/toString %))))

(defn result->map
  [result & {:as opts}]
  (latest-as-map result
                 :map-family keywordize :map-qualifier keywordize
                 :map-timestamp (:timestamp-fn opts identity)
                 :map-value (:value-fn opts #(Bytes/toString %))))

(defn rand-table []
  (str "clojure-hbase-test-db" (UUID/randomUUID)))

(deftest test-to-bytes
  (is (= :test
         (binding [*read-eval* false]
           (keyword (Bytes/toString (to-bytes :test))))))
  (doseq [val [ [1 2 3] '(1 2 3) {"1" 1} #{1 2 3}]]
    (is (= val
           (binding [*read-eval* false]
             (read-string (Bytes/toString (to-bytes val))))))))

(deftest test-create-delete-table
  (let [test-table (create-table (rand-table))
        table-names (fn [tables]
                      (reduce #(conj %1 (Bytes/toString (.getName %2)))
                              #{}
                              tables))]
    (is (contains? (table-names (adm/list-tables)) test-table)
        "Creating table")
    (drop-table test-table)
    (is (not (contains? (table-names (adm/list-tables)) test-table))
        "Drop table")))

(deftest test-add-delete-family
  (let [cf-name "test-cf-name"
        test-table (create-table (rand-table))]
     (adm/disable-table test-table)
     (adm/add-column-family test-table cf-name)
     (is (= cf-name
            (-> (adm/get-table-descriptor test-table)
                (.getFamily (to-bytes cf-name))
                (.getNameAsString)))
         "Adding new column")
     (adm/delete-column-family test-table cf-name)
     (is (nil? (-> (adm/get-table-descriptor test-table)
                   (.getFamily (to-bytes cf-name))))
         "Removing new column")
     (adm/enable-table test-table)
     (drop-table test-table)))

(deftest test-execute
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (execute htable (put* "row01" :value [:col :qual "val"]))
      (is (= [[:col :qual :ts "val"]]
             (-> htable
                 (execute (get* "row01" :column ["col" "qual"]))
                 first
                 (result->vector :timestamp-fn (fn [_] :ts))))
          "Execute Put and Get object")
      (execute htable (delete* "row01" :column ["col" "qual"]))
      (is (= []
             (-> htable
                 (execute (get* "row01" :column ["col" "qual"]))
                 first
                 (result->vector)))
          "Execute Delete and Get object"))))

(deftest test-get-put-delete
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (put htable "row01" :value ["col" "qual" "val"])
      (is (= [[:col :qual :ts "val"]]
             (-> htable
                 (get "row01" :column ["col" "qual"])
                 (result->vector :timestamp-fn (fn [_] :ts)))))
      (is (= [[:col :qual :ts "val"]]
             (-> htable
                 (get "row01" :family "col")
                 (result->vector :timestamp-fn (fn [_] :ts)))))
      (let [[col qual ts val] (-> htable
                                  (get "row01" :family "col")
                                  (result->vector)
                                  first)]
        (is (= [[:col :qual ts "val"]]
             (-> htable
                 (get "row01" :time-stamp ts)
                 (result->vector))))
        (is (= [[:col :qual ts "val"]]
             (-> htable
                 (get "row01" :time-range [(dec ts) (inc ts)])
                 (result->vector)))))
      (delete htable "row01" :column [:col :qual])
      (is (= []
             (-> htable
                 (get "row01" :column ["col" "qual"])
                 (result->vector)))))))

(deftest test-put-with-timestamp
  (with-create-table [table-name (create-table (rand-table) "col2")]
    (with-table [htable (table table-name)]
      (put htable "row-02" :with-timestamp 1337
           [:value ["col2" :qual "42"]])
      (is (= [[:col2 :qual 1337 "42"]]
             (-> (get htable "row-02" :column ["col2" "qual"])
                 (result->vector))))
      (is (= [[:col2 :qual 1337 "42"]]
             (-> (get htable "row-02" :time-stamp 1337)
                 (result->vector))))
      (is (= [[:col2 :qual 1337 "42"]]
             (-> (get htable "row-02" :time-range [1336 1338])
                 (result->vector))))
      (delete htable "row-02" :column [:col2 :qual])
      (is (= []
             (-> (get htable "row-02" :column ["col2" "qual"])
                 (result->vector)))))))

(deftest test-use-existing
  (with-create-table [table-name (create-table (rand-table) "col3")]
    (with-table [htable (table table-name)]
      (let [put-op (put* "row-03")
            put-op2 (put* "row-03" :value [:col3 :qual :hello]
                          :use-existing put-op)]
        (execute htable put-op2)
        (is (= put-op put-op2))
        (is (= [[:col3 :qual :ts "hello"]]
               (-> (get htable "row-03" :column [:col3 :qual])
                   (result->vector :timestamp-fn (fn [_] :ts))))))
      (let [get-op (get* "row-03")
            get-op2 (get* "row-03" :column [:col3 :qual]
                          :use-existing get-op)
            scan-op (scan*)
            scan-op2 (scan* :column [:col3 :qual]
                            :use-existing scan-op)]
        (is (= get-op get-op2))
        (is (= scan-op scan-op2))
        (is (= [[:col3 :qual :ts "hello"]]
               (-> (execute htable get-op2)
                   first
                   (result->vector :timestamp-fn (fn [_] :ts)))))
        (is (= [[:col3 :qual :ts "hello"]]
               (-> (execute htable scan-op2)
                   first
                   .iterator
                   iterator-seq
                   first
                   (result->vector :timestamp-fn (fn [_] :ts))))))
      (let [del-op (delete* "row-03")
            del-op2 (delete* "row-03" :column [:col3 :qual]
                             :use-existing del-op)]
        (is (= del-op del-op2))
        (execute htable del-op2)
        (is (= []
               (-> (get htable "row-03" :column [:col3 :qual])
                   (result->vector))))))))

(deftest test-multi-get-put
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (let [rows [[:col1 :qual1 :ts "val11"]
                  [:col1 :qual2 :ts "val12"]
                  [:col2 :qual1 :ts "val21"]
                  [:col2 :qual2 :ts "val22"]]]
        (put htable "row-04"
             :values [:col1 [:qual1 "val11"
                             :qual2 "val12"]
                      :col2 [:qual1 "val21"
                             :qual2 "val22"]])
        (is (= rows
               (result->vector (get htable "row-04") :timestamp-fn (fn [_] :ts))))
        (is (= (take 2 rows)
               (result->vector (get htable "row-04"
                                    :columns [:col1 [:qual1 :qual2]])
                               :timestamp-fn (fn [_] :ts))))
        (is (= (take 2 rows)
               (result->vector (get htable "row-04"
                                    :families [:col1])
                               :timestamp-fn (fn [_] :ts))))
        (delete htable "row-04" :columns [:col1 [:qual1]
                                          :col2 [:qual1]])
        (is (= [[:col1 :qual2 :ts "val12"]
                [:col2 :qual2 :ts "val21"]])
            (result->vector (get htable "row-04"
                                 :families [:col1 :col2])
                            :timestamp-fn (fn [_] :ts)))))))

(deftest test-delete-all-versions
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (put htable "row-05" :values [:col1 [:a "v1t1"
                                           :a "v1t2"
                                           :a "v1t3"
                                           :b "v2t1"]
                                    :col2 [:c "v3t1"
                                           :c "v3t2"
                                           :d "v4t1"
                                           :d "v4t2"
                                           :d "v4t3"
                                           :e "v5t1"]])
      (is (= [[:col1 :a :ts "v1t3"]
              [:col1 :b :ts "v2t1"]
              [:col2 :c :ts "v3t2"]
              [:col2 :d :ts "v4t3"]
              [:col2 :e :ts "v5t1"]]
             (result->vector (get htable "row-05")
                             :timestamp-fn (fn [_] :ts))))
      (delete htable "row-05" :all-versions [:column [:col1 :a]])
      (is (= [[:col1 :b :ts "v2t1"]]
             (result->vector (get htable "row-05" :family :col1)
                             :timestamp-fn (fn [_] :ts))))
      (delete htable "row-05" :all-versions [:column [:col1 :b]
                                             :columns [:col2 [:c :d]]])
      (is (= []
             (result->vector (get htable "row-05" :family :col1)
                             :timestamp-fn (fn [_] :ts))))
      (is (= [[:col2 :e :ts "v5t1"]]
             (result->vector (get htable "row-05" :family :col2)
                             :timestamp-fn (fn [_] :ts))))
      (put htable "row-05" :values [:col1 [:a "final"]])
      (is (= [[:col1 :a :ts "final"]]
             (result->vector (get htable "row-05" :family :col1)
                             :timestamp-fn (fn [_] :ts)))))))

(deftest test-op-with-timestamp
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (put htable "row-06"
           :with-timestamp 1 [:value [:col1 :a :v1]]
           :with-timestamp 2 [:value [:col1 :b :v2]]
           :with-timestamp 3 [:value [:col1 :c :v3]]
           :with-timestamp 4 [:value [:col2 :d :v4]]
           :with-timestamp 5 [:value [:col2 :e :v5]]
           :with-timestamp 6 [:value [:col2 :f :v6]])
      (is (= [[:col1 :a :ts "v1"]
              [:col1 :b :ts "v2"]
              [:col1 :c :ts "v3"]
              [:col2 :d :ts "v4"]
              [:col2 :e :ts "v5"]
              [:col2 :f :ts "v6"]]
             (result->vector (get htable "row-06")
                             :timestamp-fn (fn [_] :ts))))
      (delete htable "row-06" :with-timestamp 4 [:column [:col2 :d]
                                                 :columns [:col1 [:a :b]]])
      (is (= [[:col1 :a :ts "v1"]
              [:col1 :b :ts "v2"]
              [:col1 :c :ts "v3"]
              [:col2 :e :ts "v5"]
              [:col2 :f :ts "v6"]]
             (result->vector (get htable "row-06")
                             :timestamp-fn (fn [_] :ts))))
      (delete htable "row-06" :with-timestamp-before 4
              [:column [:col2 :e]
               :columns [:col1 [:b :c]]])
      (is (= [[:col1 :a :ts "v1"]
              [:col2 :e :ts "v5"]
              [:col2 :f :ts "v6"]]
             (result->vector (get htable "row-06")
                             :timestamp-fn (fn [_] :ts)))))))

(deftest test-atomic-ops
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (check-and-put htable "row-07" :col :qual nil
                     (put* "row-07" :value [:col :qual "val"]))
      (is (= [[:col :qual :ts "val"]]
             (result->vector (get htable "row-07")
                             :timestamp-fn (fn [_] :ts))))
      (check-and-put htable "row-07" :col :qual "val"
                     (put* "row-07" :value [:col :qual2 "val1"]))
      (is (= [[:col :qual :ts "val"]
              [:col :qual2 :ts "val1"]]
             (result->vector (get htable "row-07")
                             :timestamp-fn (fn [_] :ts))))
      (check-and-delete htable "row-07" :col :qual nil
                        (delete* "row-07" :column [:col :qual2]))
      (is (= [[:col :qual :ts "val"]
              [:col :qual2 :ts "val1"]]
             (result->vector (get htable "row-07")
                             :timestamp-fn (fn [_] :ts))))
      (check-and-delete htable "row-07" :col :qual "val"
                        (delete* "row-07" :column [:col :qual2]))
      (is (= [[:col :qual :ts "val"]]
             (result->vector (get htable "row-07")
                             :timestamp-fn (fn [_] :ts))))
      (check-and-delete htable "row-07" :col :qual2 nil
                        (delete* "row-07" :column [:col :qual]))
      (is (= []
             (result->vector (get htable "row-07")
                             :timestamp-fn (fn [_] :ts)))))))

(deftest test-scan-ordering
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (let [rows (sort-by first (for [k (range 1000)]
                                  [(str (UUID/randomUUID))
                                   (str (UUID/randomUUID))]))]
        (doseq [[k v] rows]
          (put htable k :value [:col :qual v]))
        (is (= (map first rows)
               (with-scanner [res (scan htable)]
                 (map #(Bytes/toString (.getRow %)) (seq res)))))))))

(deftest test-scan-qualifiers
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (put htable 1 :values [:col1 [:a "1" :b "2" :c "3" :d "4"]])
      (put htable 2 :values [:col1 [:a "5" :b "6" :c "7" :d "8"]])
      (put htable 3 :values [:col2 [:z "5" :y "4" :x "3" :w "2"]])
      (put htable 4 :values [:col2 [:z "2" :y "3" :x "4" :w "5"]])
      (testing "scan subset of qulifiers"
        (is (= [{:col1 {:a "1"}}
                {:col1 {:a "5"}}]
               (with-scanner [res (scan htable :column [:col1 :a])]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))
        (is (= [{:col1 {:a "1" :b "2"}}
                {:col1 {:a "5" :b "6"}}]
               (with-scanner [res (scan htable :columns [:col1 [:a :b]])]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))
        (is (= []
               (with-scanner [res (scan htable :columns [:col1 [:y :z]])]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))))))

(deftest test-scan-families
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (put htable 1 :values [:col1 [:a "1" :b "2" :c "3" :d "4"]])
      (put htable 2 :values [:col1 [:a "5" :b "6" :c "7" :d "8"]])
      (put htable 3 :values [:col2 [:z "5" :y "4" :x "3" :w "2"]])
      (put htable 4 :values [:col2 [:z "2" :y "3" :x "4" :w "5"]])
      (testing "scan column families"
        (is (= [{:col1 {:a "1" :b "2" :c "3" :d "4"}}
                {:col1 {:a "5" :b "6" :c "7" :d "8"}}]
               (with-scanner [res (scan htable :family :col1)]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))
        (is (= [{:col1 {:a "1" :b "2" :c "3" :d "4"}}
                {:col1 {:a "5" :b "6" :c "7" :d "8"}}]
               (with-scanner [res (scan htable :families [:col1])]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))
        (is (= [{:col1 {:a "1" :b "2" :c "3" :d "4"}}
                {:col1 {:a "5" :b "6" :c "7" :d "8"}}
                {:col2 {:z "5" :y "4" :x "3" :w "2"}}
                {:col2 {:z "2" :y "3" :x "4" :w "5"}}]
               (with-scanner [res (scan htable :families [:col1 :col2])]
                 (->> (.iterator res)
                      iterator-seq
                      (map result->map)
                      doall))))))))

(deftest test-scan-time-range
  (with-create-table [table-name (create-table (rand-table) "col1" "col2")]
    (with-table [htable (table table-name)]
      (put htable 1 :values [:col1 [:a "1"]])
      (put htable 2 :values [:col1 [:a "2"]])
      (put htable 3 :values [:col2 [:z "3"]])
      (put htable 4 :values [:col2 [:z "4"]])
      (let [timestamps (with-scanner [res (scan htable)]
                         (->> (.iterator res)
                              iterator-seq
                              (map (comp #(nth % 2) first result->vector))
                              doall))]
        (testing "time-range scan is upper bound exclusive"
          (is (= [{:col1 {:a "1"}}
                  {:col1 {:a "2"}}
                  {:col2 {:z "3"}}]
                 (with-scanner [res (scan htable :time-range [(apply min timestamps)
                                                              (apply max timestamps)])]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall))))
          (is (= [{:col1 {:a "1"}}
                  {:col1 {:a "2"}}
                  {:col2 {:z "3"}}
                  {:col2 {:z "4"}}]
                 (with-scanner [res (scan htable :time-range [(apply min timestamps)
                                                              (inc (apply max timestamps))])]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall)))))
        (testing "scan by timestamp"
          (is (= [{:col1 {:a "1"}}]
                 (with-scanner [res (scan htable :time-stamp (first timestamps))]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall)))))
        (testing "scan by row key range"
          (is (= [{:col2 {:z "3"}}
                  {:col2 {:z "4"}}]
                 (with-scanner [res (scan htable :start-row 3)]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall))))
          ;; stop-row is exclusive too
          (is (= [{:col1 {:a "1"}}
                  {:col1 {:a "2"}}]
                 (with-scanner [res (scan htable :stop-row 3)]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall))))
          (is (= [{:col1 {:a "2"}}]
                 (with-scanner [res (scan htable :start-row 2 :stop-row 3)]
                   (->> (.iterator res)
                        iterator-seq
                        (map result->map)
                        doall)))))))))

(deftest test-as-map
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (put htable "row-08" :value [:col :qual "val"])
      (is (= {:col {:qual {1 "val"}}}
             (-> (get htable "row-08")
                 (as-map :map-family keywordize
                         :map-qualifier keywordize
                         :map-value #(Bytes/toString %)
                         :map-timestamp (fn [_] 1)))))
      (is (= {:col {:qual "val"}}
             (-> (get htable "row-08")
                 (latest-as-map :map-family keywordize
                                :map-qualifier keywordize
                                :map-value #(Bytes/toString %)
                                :map-timestamp identity)))))))

(deftest test-reverse-scan
  (with-create-table [table-name (create-table (rand-table) "col")]
    (with-table [htable (table table-name)]
      (put htable 1 :values [:col [:a "1" :b "bar1"]])
      (put htable 2 :values [:col [:a "2" :b "bar2"]])
      (put htable 3 :values [:col [:c "3"]])
      (put htable 4 :values [:col [:d "4"]])
      (is (= [{:col {:a "1" :b "bar1"}}
              {:col {:a "2" :b "bar2"}}
              {:col {:c "3"}}
              {:col {:d "4"}}]
             (with-scanner [res (scan htable)]
               (->> (.iterator res)
                    iterator-seq
                    (map result->map)
                    doall))))
      (is (= [{:col {:d "4"}}
              {:col {:c "3"}}
              {:col {:a "2" :b "bar2"}}
              {:col {:a "1" :b "bar1"}}]
             (with-scanner [res (scan htable :reversed true)]
               (->> (.iterator res)
                    iterator-seq
                    (map result->map)
                    doall))))
      ;; stop row is exclusive
      (is (= [{:col {:d "4"}}
              {:col {:c "3"}}
              {:col {:a "2" :b "bar2"}}]
             (with-scanner [res (scan htable
                                      :start-row 4
                                      :stop-row 1
                                      :reversed true)]
               (->> (.iterator res)
                    iterator-seq
                    (map result->map)
                    doall)))))))
