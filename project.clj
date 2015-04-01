(defproject clojure-hbase "0.96.1.1-cdh5.0.5"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.hbase/hbase-common "0.96.1.1-cdh5.0.5"]
                 [org.apache.hbase/hbase-client "0.96.1.1-cdh5.0.5"]
                 [org.apache.hbase/hbase-server "0.96.1.1-cdh5.0.5"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.apache.hadoop/hadoop-test "2.3.0-mr1-cdh5.0.5" :scope "test"]]
  :repositories [["cloudera" "https://repository.cloudera.com/content/groups/public/"]])
