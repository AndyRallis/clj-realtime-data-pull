(ns realtime-data-pull.core
  (:require [java-time :as t]
            [realtime-data-pull.url-utilities :as util]))

(def config (atom (read-string (slurp "config.edn"))))

(defn -main
  "pull in api data"
  [n-go-blocks days]
  (println "Beginning processesing")
  (println (t/local-date-time))
  (let [[url-blocks-live write-blocks-live] 
        (util/run-the-thing
         (Long/valueOf n-go-blocks)
         (Long/valueOf days)
         config)]
    (util/system-monitor-and-shutdown 
     url-blocks-live
     write-blocks-live)))
