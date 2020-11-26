(ns realtime-data-pull.url-utilities
    (:require [clj-http.client :as client]
            [cheshire.core :as cs]
            [java-time :as t]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [clojure.java.io :as io]))


(defn all-date-intervals [start-time]
  (let [start-date-as-date (apply t/zoned-date-time start-time)
        starts (map #(t/instant (t/with-zone % "UTC"))
                    (iterate #(t/plus % (t/days 1)) start-date-as-date))
        ends (rest starts)]
    (map #(hash-map :modifiedFrom %1 :modifiedBefore %2) starts ends)))

(defn build-target-urls [base-url uris]
  (map #(vector (str base-url "/2/" %) %) uris))

(defn create-urls [base-url param-defaults time-start]
  (let [url-map (map conj (repeat param-defaults) (all-date-intervals time-start))
        keys (map #(map name (keys %)) url-map)
        vals (map vals url-map)]
    (map #(str base-url "?"
               (str/join "&" (map
                              (fn [a b] (str/join "=" (vector a b)))
                              %1 %2))) keys vals)))

(defn apply-urls [urls days param-defaults time-start]
  (mapcat (fn [[base-url url-name]]
            (let [url-list (take days (create-urls base-url param-defaults time-start))]
              (map vector url-list (repeat url-name)))) urls))


(defn load-initial-chan [coll ch]
  (async/onto-chan ch coll false))

(defn get-bearer-token
  [auth-url credentials]
  (let [body-edn
        (cs/decode
         (:body (client/post auth-url
                             {:form-params credentials
                              :content-type :x-www-form-urlencoded})) true)
        token-type (:token_type body-edn)
        token (:access_token body-edn)]
    (str token-type " " token)))

(defn parse-next [resp]
  (:href (first (filter #(= (:rel %) "next") (:links resp)))))

(defn handle-success [ch-url ch-data file resp]
  (let [resp-parsed (cs/decode (:body resp) true)]
    (async/go (async/>! ch-data (vector (:data resp-parsed) file)))
    (when-let [next-url (parse-next resp-parsed)]
      (async/go (async/>! ch-url (vector next-url file))))))

(defn handle-failure [ch-url retry-url file _]
  (async/go (async/>! ch-url (vector retry-url file))))

(defn configure-url-call [token]
  (fn [url-with-params ch-url ch-data file]
    (client/get url-with-params
                {:headers {:authorization token}
                 :async? true}
                (partial handle-success ch-url ch-data file)
                (partial handle-failure ch-url url-with-params file))))

(defn writelines [file-path data]
  (with-open [writer (io/writer file-path :append true)]
    (.write writer (str data "\r\n"))))

(defn channel-watch-for-spindown
  [key _ old-value _]
  (println "Timeout:" (name key) "Channel" old-value " spinning down"))

(defn system-monitor-and-shutdown [url-blocks-live write-blocks-live]
  (loop []
    (if (= (+ @url-blocks-live @write-blocks-live) 0)
      (do
        (println "All data processed, exiting normally")
        (println (t/local-date-time))
        (System/exit 0))
      (recur))))

(defn run-the-thing [n-go-blocks days config]
  (let [url-blocks-live (atom 0)
        write-blocks-live (atom 0)
        {:keys [base-url uris auth-url credentials 
                param-defaults start-time working-path keys-to-pull]} @config]
  (swap! url-blocks-live + n-go-blocks)
  (swap! write-blocks-live + n-go-blocks)
  (add-watch url-blocks-live :URL-blocks channel-watch-for-spindown)
  (add-watch write-blocks-live :write-blocks channel-watch-for-spindown)
  (let [res-chan (async/chan)
        counter (atom {:call-counter 0 :write-counter 0})]
    (let [url-chan (async/chan) 
          urls (build-target-urls base-url uris)
          bearer-token (get-bearer-token auth-url credentials)
          get-api-data-by-page (configure-url-call bearer-token)]
      (load-initial-chan (apply-urls urls days param-defaults start-time) url-chan)
      ;; make sure URL channel is full
      (delay 5000)
      (dotimes [_ n-go-blocks]
        (async/go-loop []
          (if-let [url-file (first (async/alts! [url-chan (async/timeout 60000)]))]            
            (let [url (first url-file)
                  file (second url-file)]
              (get-api-data-by-page url url-chan res-chan file)
              (swap! counter update :call-counter inc)
             (println "Processed " (:call-counter @counter) " @ URL: " url)
              (recur))
            (swap! url-blocks-live dec)))))
    ;; make sure results are flowing through
    (delay 5000)
    (dotimes [x n-go-blocks]
      (async/go-loop []
        (if-let [data-file (first (async/alts! [res-chan (async/timeout 60000)]))]
          (let [data (map #(select-keys % keys-to-pull) (first data-file))
                file (second data-file)]
            (writelines (str working-path file x ".edn") (pr-str data))
            (swap! counter update :write-counter inc)
            (println "Output " (:write-counter @counter) " data pulls")
            (recur))
          (swap! write-blocks-live dec))))
    [url-blocks-live write-blocks-live])))