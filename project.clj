(defproject cn.leancloud/clojure-hbase "1.2.0-cdh5.7.4"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.hbase/hbase-common "1.2.0-cdh5.7.4"]
                 [org.apache.hbase/hbase-client "1.2.0-cdh5.7.4"]
                 [org.apache.hbase/hbase-server "1.2.0-cdh5.7.4"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.2.3"]]
  :repositories [["cloudera" "https://repository.cloudera.com/content/groups/public/"]]
  :profiles {:dev {:dependencies [[org.apache.hbase/hbase-testing-util "1.2.0-cdh5.7.4"]]}})
