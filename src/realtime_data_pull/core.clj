(ns realtime-data-pull.core
  (:require [clj-http.client :as client]
            [cheshire.core :as cs]
            [java-time :as t]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [clojure.java.io :as io]))

(def config (read-string (slurp "config.edn")))

(def BASE_URL (:base-url config))
(def AUTH-URL (:auth-url config))
(def CREDENTIALS (:credentials config))
(def PARAM-DEFAULTS (:param-defaults config))
(def URIS (:uris config))
(def WORKING-PATH (:working-path config))
(def keys-to-pull (:keys-to-pull config))
(def TIME-START (apply t/zoned-date-time (:start-time config)))

(def url-blocks-live (atom 0))
(def write-blocks-live (atom 0))


(def all-date-intervals (let [starts (map #(t/instant (t/with-zone % "UTC"))
                                          (iterate #(t/plus % (t/days 1)) TIME-START))
                              ends (rest starts)]
                          (map #(hash-map :modifiedFrom %1 :modifiedBefore %2)  starts ends)))

(def urls (map #(vector (str BASE_URL "/2/" %) %) URIS))

(defn create-urls [base-url]
  (let [url-map (map conj (repeat PARAM-DEFAULTS) all-date-intervals)
        keys (map #(map name (keys %)) url-map)
        vals (map vals url-map)]
    (map #(str base-url "?"
               (str/join "&" (map
                              (fn [a b] (str/join "=" (vector a b)))
                              %1 %2))) keys vals)))
(defn apply-urls [urls days]
  (mapcat (fn [[base-url url-name]]
            (let [url-list (take days (create-urls base-url))]
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

(defn system-monitor-and-shutdown []
  (loop []
    (if (= (+ @url-blocks-live @write-blocks-live) 0)
      (do
        (println "All data processed, exiting normally")
        (println (t/local-date-time))
        (System/exit 0))
      (recur))))

(defn run-the-thing [n-go-blocks days]
  (swap! url-blocks-live + n-go-blocks)
  (swap! write-blocks-live + n-go-blocks)
  (add-watch url-blocks-live :URL-blocks channel-watch-for-spindown)
  (add-watch write-blocks-live :write-blocks channel-watch-for-spindown)
  (let [res-chan (async/chan)
        counter (atom {:call-counter 0 :write-counter 0})]
    (let [url-chan (async/chan)
          bearer-token (get-bearer-token AUTH-URL CREDENTIALS)
          get-api-data-by-page (configure-url-call bearer-token)]
      (load-initial-chan (apply-urls urls days) url-chan)
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
            (writelines (str WORKING-PATH file x ".edn") (pr-str data))
            (swap! counter update :write-counter inc)
            (println "Output " (:write-counter @counter) " data pulls")
            (recur))
          (swap! write-blocks-live dec))))))


(defn -main
  "pull in api data"
  [n-go-blocks days]
  (println "Beggining processesing")
  (println (t/local-date-time))
  (run-the-thing
   (Long/valueOf n-go-blocks)
   (Long/valueOf days))
  (system-monitor-and-shutdown))
