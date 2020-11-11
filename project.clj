(defproject realtime-data-pull "0.1.0-SNAPSHOT"
  :description "Custom data pull to query APIs realtime from a mobile wellness application"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clj-http "3.10.1"]
                 [cheshire "5.10.0"]
                 [clojure.java-time "0.3.2"]
                 [org.clojure/core.async "1.3.610"]
                 [org.clojure/data.csv "1.0.0"]]
  :repl-options {:init-ns realtime-data-pull.core}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :jvm-opts ["-Xmx6g" "-server"])
