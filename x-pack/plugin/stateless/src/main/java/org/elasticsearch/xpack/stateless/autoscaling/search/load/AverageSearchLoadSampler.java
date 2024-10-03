/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;

public class AverageSearchLoadSampler {
    static final double DEFAULT_SEARCH_EWMA_ALPHA = 0.2;
    static final double DEFAULT_SHARD_READ_EWMA_ALPHA = 0.6;

    public static final Setting<Double> SEARCH_LOAD_SAMPLER_EWMA_ALPHA_SETTING = Setting.doubleSetting(
        "serverless.autoscaling.search.sampler.search_load_ewma_alpha",
        DEFAULT_SEARCH_EWMA_ALPHA,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );
    public static final Setting<Double> SHARD_READ_SAMPLER_EWMA_ALPHA_SETTING = Setting.doubleSetting(
        "serverless.autoscaling.search.sampler.shard_read_load_ewma_alpha",
        DEFAULT_SHARD_READ_EWMA_ALPHA,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    public static final String SEARCH_EXECUTOR = Names.SEARCH;
    public static final String SHARD_READ_EXECUTOR = SHARD_READ_THREAD_POOL;

    /**
     * Stores the monitored executor names and EWMA alpha settings.
     */
    static final Map<String, Setting<Double>> MONITORED_EXECUTORS = Map.of(
        SEARCH_EXECUTOR,
        SEARCH_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
        SHARD_READ_EXECUTOR,
        SHARD_READ_SAMPLER_EWMA_ALPHA_SETTING
    );

    private final ThreadPool threadPool;
    private final Map<String, AverageLoad> averageThreadLoadPerExecutor = new HashMap<>();
    private final double numProcessors;

    public static AverageSearchLoadSampler create(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        assert MONITORED_EXECUTORS.keySet()
            .stream()
            .map(threadPool::executor)
            .allMatch(executor -> executor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor);

        Map<String, Double> threadPoolsAndEWMAAlpha = new HashMap<>(MONITORED_EXECUTORS.size(), 1.0f);
        MONITORED_EXECUTORS.forEach((name, setting) -> threadPoolsAndEWMAAlpha.put(name, clusterSettings.get(setting)));
        var sampler = new AverageSearchLoadSampler(
            threadPool,
            SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.get(settings),
            threadPoolsAndEWMAAlpha,
            EsExecutors.nodeProcessors(settings).count()
        );
        MONITORED_EXECUTORS.forEach(
            (name, setting) -> clusterSettings.addSettingsUpdateConsumer(setting, value -> sampler.updateEWMAAlpha(name, value))
        );
        return sampler;
    }

    AverageSearchLoadSampler(
        ThreadPool threadPool,
        TimeValue samplingFrequency,
        Map<String, Double> threadPoolToAlphaValue,
        double numProcessors
    ) {
        assert threadPoolToAlphaValue.keySet().equals(MONITORED_EXECUTORS.keySet())
            : "threadPoolToAlphaValue must contain a single entry for each of the monitored executors: " + MONITORED_EXECUTORS;

        this.threadPool = threadPool;
        threadPoolToAlphaValue.forEach(
            (name, alpha) -> averageThreadLoadPerExecutor.put(
                name,
                new AverageLoad(threadPool.info(name).getMax(), samplingFrequency, alpha)
            )
        );
        this.numProcessors = numProcessors;
    }

    public void sample() {
        for (var name : MONITORED_EXECUTORS.keySet()) {
            var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(name);
            long currentTotalNanos = executor.getTotalTaskExecutionTime();
            var ongoingTasks = executor.getOngoingTasks();
            averageThreadLoadPerExecutor.get(name).update(currentTotalNanos, ongoingTasks.values());
        }
    }

    public ExecutorLoadStats getExecutorLoadStats(String executorName) {
        if (MONITORED_EXECUTORS.get(executorName) == null) {
            throw new IllegalArgumentException("only the following executors are valid: " + MONITORED_EXECUTORS);
        }
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(executorName);
        return new ExecutorLoadStats(
            averageThreadLoadPerExecutor.get(executorName).get(),
            executor.getTaskExecutionEWMA(),
            executor.getCurrentQueueSize(),
            executor.getMaximumPoolSize(),
            numProcessors
        );
    }

    private void updateEWMAAlpha(String executorName, double alpha) {
        if (MONITORED_EXECUTORS.get(executorName) == null) {
            throw new IllegalArgumentException("only the following executors are valid: " + MONITORED_EXECUTORS);
        }
        AverageLoad averageLoad = averageThreadLoadPerExecutor.get(executorName);
        averageLoad.updateEwmaAlpha(alpha);
    }

    /**
     * Represents the average number of threads busy per second for an executor. Each sample (1 second interval) represents the proportion
     * of time (wall clock) spent running tasks. We use a weighted average and the EWMA alpha value (0.2) is chosen such that samples older
     * than 1 minute have a negligible effect on the average load.
     */
    static class AverageLoad {
        private volatile long previousTotalExecutionTimeNanos = 0;
        private ExponentiallyWeightedMovingAverage searchLoadEwma;
        // The maximum execution time possible within a sampling period, which is samplingFrequency * threadpoolSize
        private final long maxExecutionTimePerSamplingPeriodNanos;
        private final TimeValue samplingFrequency;

        AverageLoad(int executorMaxSize, TimeValue samplingFrequency, double EWMAAlpha) {
            this.maxExecutionTimePerSamplingPeriodNanos = executorMaxSize * samplingFrequency.nanos();
            this.samplingFrequency = samplingFrequency;
            this.searchLoadEwma = new ExponentiallyWeightedMovingAverage(EWMAAlpha, 0.0);
        }

        public void update(long currentTotalExecutionTimeNanos, Collection<Long> ongoingTasksStartNanos) {
            update(currentTotalExecutionTimeNanos, ongoingTasksStartNanos, System.nanoTime());
        }

        void update(long currentTotalExecutionTimeNanos, Collection<Long> ongoingTasksStartNanos, final long nowNanos) {
            final long previous = previousTotalExecutionTimeNanos;
            long taskExecutionTimeSinceLastSample = calculateTaskExecutionTimeSinceLastSample(
                currentTotalExecutionTimeNanos,
                previous,
                ongoingTasksStartNanos,
                nowNanos
            );
            synchronized (this) {
                searchLoadEwma.addValue((double) taskExecutionTimeSinceLastSample / samplingFrequency.nanos());
            }
            previousTotalExecutionTimeNanos = currentTotalExecutionTimeNanos;
        }

        long calculateTaskExecutionTimeSinceLastSample(
            long currentTotalExecutionTimeNanos,
            long previousTotalExecutionTimeNanos,
            Collection<Long> ongoingTasksStartNanos,
            long nowNanos
        ) {
            assert currentTotalExecutionTimeNanos >= previousTotalExecutionTimeNanos;
            // Cap current execution time of each running tasks to the sampling period, because even if a task
            // has been running for several sampling periods, all we care about is how much that task contributes
            // to the accumulated task execution time of the current sampling period, and this cannot be more than
            // the sampling period itself.
            long executionTimeOfOngoingTasks = ongoingTasksStartNanos.stream()
                .mapToLong(t -> ensureRange(nowNanos - t, 0, samplingFrequency.nanos()))
                .sum();
            assert executionTimeOfOngoingTasks >= 0;
            // We also cap the total task execution time within a sampling period to the max possible value which is
            // threadpool_size * sampling_period. This is necessary since for the estimation we use the currentTotalExecutionTimeNanos
            // that is reported by the threadpool and the list of ongoing tasks. However, these two are not guaranteed to be exclusive
            // with respect to the tasks that are executed.
            // This means that it is possible that a task is finished, its execution time is included in currentTotalExecutionTimeNanos,
            // but it is not removed from the list of ongoing tasks yet, and therefore we might double count it since
            // {@code org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor.getOngoingTasks} ensures that
            // we first add the task execution time to the total before removing it from ongoing tasks.
            var taskExecutionTimeSinceLastSample = ensureRange(
                currentTotalExecutionTimeNanos - previousTotalExecutionTimeNanos + executionTimeOfOngoingTasks,
                0,
                maxExecutionTimePerSamplingPeriodNanos
            );
            assert taskExecutionTimeSinceLastSample >= 0 && taskExecutionTimeSinceLastSample <= maxExecutionTimePerSamplingPeriodNanos;
            return taskExecutionTimeSinceLastSample;
        }

        synchronized void updateEwmaAlpha(double ewmaAlpha) {
            searchLoadEwma = new ExponentiallyWeightedMovingAverage(ewmaAlpha, searchLoadEwma.getAverage());
        }

        /**
         * Returns the average number of threads busy within the past 1 minute. E.g. 2.0 means roughly 2 threads were busy within the past
         * minute (including IO time).
         */
        public synchronized double get() {
            return searchLoadEwma.getAverage();
        }
    }

    static long ensureRange(long n, long min, long max) {
        return Math.max(min, Math.min(n, max));
    }
}
