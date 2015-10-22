(defproject clojure-hbase "0.98.6-cdh5.3.6"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.hbase/hbase-common "0.98.6-cdh5.3.6"]
                 [org.apache.hbase/hbase-client "0.98.6-cdh5.3.6"]
                 [org.apache.hbase/hbase-server "0.98.6-cdh5.3.6"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.apache.hadoop/hadoop-test "2.5.0-mr1-cdh5.3.6" :scope "test"]]
  :repositories [["cloudera" "https://repository.cloudera.com/content/groups/public/"]])
