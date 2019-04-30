(ns microbe.util
  "Utility functions useful for micro benchmarking"
  (:import (java.util.concurrent ArrayBlockingQueue ExecutorService
                                 ThreadFactory ThreadLocalRandom
                                 ThreadPoolExecutor
                                 ThreadPoolExecutor$CallerRunsPolicy TimeUnit)))

(defn ticker
  "Returns a lazy sequence that generates values in the given frequency

    ;;; Will take 10 seconds to finish
    (doseq [_ (take 10000 (ticker 1000))]
      (do something))"
  [frequency & [reset]]
  (let [reset    (or reset 10)
        reset-ns (long (* reset (Math/pow 10 9)))
        interval (long (quot (Math/pow 10 9) frequency))
        tick-fn  (fn tick-fn
                   [started ticks all-ticks]
                   (lazy-seq
                    (let [now     (System/nanoTime)
                          diff-ms (quot (- (+ started (* (unchecked-inc ticks)
                                                         interval)) now)
                                        1000000)]
                      (cons (do
                              (when (> diff-ms 10) (Thread/sleep diff-ms))
                              all-ticks)
                            (if (>= (- now started) reset-ns)
                              (tick-fn (System/nanoTime)
                                       0
                                       (unchecked-inc all-ticks))
                              (tick-fn started
                                       (unchecked-inc ticks)
                                       (unchecked-inc all-ticks)))))))]
    (tick-fn (System/nanoTime) 0 0)))

(let [range-incl #(range %1 (inc %2))
      chars (vec (mapcat #(map char (apply range-incl (map int %)))
                         [[\a \z] [\A \Z] [\0 \9]])) ; base62
      achars (char-array chars)]
  (defn rand-str
    "Returns a random base62 string of the given length."
    [n]
    (let [cnt (count achars)
          rng (ThreadLocalRandom/current) ; 3-times faster than rand-int
          arr (char-array n)]
      (dotimes [idx n]
        (aset arr idx (aget achars (.nextInt rng 0 cnt))))
      (String/valueOf arr))))

;;; Blocking thread pool
(let [pool-count (atom 0)]
  (defn ^ExecutorService create-thread-pool
    [size]
    (when-not (and (integer? size) (pos? size))
      (throw (IllegalArgumentException. "Number of threads should be a positive integer")))
    (ThreadPoolExecutor.
     size size Long/MAX_VALUE TimeUnit/SECONDS
     (ArrayBlockingQueue. 1000)
     (let [pool-num     (swap! pool-count inc)
           thread-count (atom 0)]
       (reify ThreadFactory
         (newThread [this runnable]
           (Thread. runnable (format "microbe-pool%d-%d"
                                     pool-num (swap! thread-count inc))))))
     (ThreadPoolExecutor$CallerRunsPolicy.))))

(defmacro with-thread-pool
  "bindings => [name execution-count-or-seqable :threads num-threads]

  Evaluates the forms multiple times or for each item in the sequence using
  a thread pool. If the number of threads is not specified, it is set to the
  number of available cores on the system."
  [bindings & forms]
  (when-not (vector? bindings)
    (throw (IllegalArgumentException. "requires vector as the first argument")))
  (let [pairs (group-by (comp #{:threads} first) (partition 2 bindings))
        [sym times] (last (pairs nil))
        num-threads (some->> (pairs :threads) last last)
        num-threads (or num-threads
                        (.. Runtime getRuntime availableProcessors))]
    (when-not (symbol? sym)
      (throw (IllegalArgumentException. "binding required")))
    `(let [pool# (create-thread-pool ~num-threads)
           times# ~times]
       (try
         (if (integer? times#)
           (dotimes [~sym times#] (.submit pool# ^Callable #(do ~@forms)))
           (doseq [~sym times#] (.submit pool# ^Callable #(do ~@forms))))
         (finally
           (.shutdown pool#)
           (.awaitTermination pool# Long/MAX_VALUE java.util.concurrent.TimeUnit/SECONDS))))))

(comment
  (with-thread-pool [idx 100 :threads 10]
    (println (str "(" idx ")")))
  (with-thread-pool [item (range 100) :threads 10]
    (println (str "(" item ")"))))
