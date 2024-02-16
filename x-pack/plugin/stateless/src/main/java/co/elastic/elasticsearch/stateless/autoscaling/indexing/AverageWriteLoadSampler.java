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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AverageWriteLoadSampler {

    static final Set<String> WRITE_EXECUTORS = Set.of(Names.WRITE, Names.SYSTEM_WRITE, Names.SYSTEM_CRITICAL_WRITE);
    static final double DEFAULT_EWMA_ALPHA = 0.2;
    public static final Setting<Double> WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING = Setting.doubleSetting(
        "serverless.autoscaling.indexing.sampler.write_load_ewma_alpha",
        DEFAULT_EWMA_ALPHA,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final ThreadPool threadPool;
    private final Map<String, AverageLoad> averageLoadPerExector = new HashMap<>();

    public static AverageWriteLoadSampler create(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        assert WRITE_EXECUTORS.stream()
            .map(threadPool::executor)
            .allMatch(executor -> executor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor);
        double ewmaAlpha = clusterSettings.get(WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING);
        var sampler = new AverageWriteLoadSampler(threadPool, IngestLoadSampler.SAMPLING_FREQUENCY_SETTING.get(settings), ewmaAlpha);
        clusterSettings.addSettingsUpdateConsumer(WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING, sampler::updateEWMAAlpha);
        return sampler;
    }

    AverageWriteLoadSampler(ThreadPool threadPool, TimeValue samplingFrequency, double ewmaAlpha) {
        this.threadPool = threadPool;
        WRITE_EXECUTORS.forEach(
            name -> averageLoadPerExector.put(name, new AverageLoad(threadPool.info(name).getMax(), samplingFrequency, ewmaAlpha))
        );
    }

    public void sample() {
        for (var name : WRITE_EXECUTORS) {
            var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(name);
            long currentTotalNanos = executor.getTotalTaskExecutionTime();
            var ongoingTasks = executor.getOngoingTasks();
            averageLoadPerExector.get(name).update(currentTotalNanos, ongoingTasks.values());
        }
    }

    public ExecutorStats getExecutorStats(String executorName) {
        if (WRITE_EXECUTORS.contains(executorName) == false) {
            throw new IllegalArgumentException("only the following write executors are valid: " + WRITE_EXECUTORS);
        }
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(executorName);
        return new ExecutorStats(
            averageLoadPerExector.get(executorName).get(),
            executor.getTaskExecutionEWMA(),
            executor.getCurrentQueueSize(),
            executor.getMaximumPoolSize()
        );
    }

    private void updateEWMAAlpha(double alpha) {
        averageLoadPerExector.forEach((s, averageLoad) -> averageLoad.updateEwmaAlpha(alpha));
    }

    /**
     * Represents the average number of threads busy per second for an executor.
     * Each sample (1 second interval) represents the proportion of time (wall clock) spent running tasks. We use a weighted average and
     * the EWMA alpha value (0.2) is chosen such that samples older than 1 minute have a negligible effect on the average load.
     */
    static class AverageLoad {
        private volatile long previousTotalExecutionTimeNanos = 0;
        private ExponentiallyWeightedMovingAverage writeLoadEwma;
        // The maximum execution time possible within a sampling period, which is samplingFrequency * threadpoolSize
        private final long maxExecutionTimePerSamplingPeriodNanos;
        private final TimeValue samplingFrequency;

        AverageLoad(int executorMaxSize, TimeValue samplingFrequency, double EWMAAlpha) {
            this.maxExecutionTimePerSamplingPeriodNanos = executorMaxSize * samplingFrequency.nanos();
            this.samplingFrequency = samplingFrequency;
            this.writeLoadEwma = new ExponentiallyWeightedMovingAverage(EWMAAlpha, 0.0);
        }

        public void update(long currentTotalExecutionTimeNanos, Collection<Long> ongoingTasksStartNanos) {
            final long nowNanos = System.nanoTime();
            final long previous = previousTotalExecutionTimeNanos;
            long taskExecutionTimeSinceLastSample = calculateTaskExecutionTimeSinceLastSample(
                currentTotalExecutionTimeNanos,
                previous,
                ongoingTasksStartNanos,
                nowNanos
            );
            synchronized (this) {
                writeLoadEwma.addValue((double) taskExecutionTimeSinceLastSample / samplingFrequency.nanos());
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
                .map(t -> ensureRange(nowNanos - t, 0, samplingFrequency.nanos()))
                .mapToLong(Long::longValue)
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
            writeLoadEwma = new ExponentiallyWeightedMovingAverage(ewmaAlpha, writeLoadEwma.getAverage());
        }

        /**
         * Returns the average number of threads busy within the past 1 minute.
         * E.g. 2.0 means roughly 2 threads were busy within the past minute (including IO time).
         */
        public synchronized double get() {
            return writeLoadEwma.getAverage();
        }
    }

    static long ensureRange(long n, long min, long max) {
        return Math.max(min, Math.min(n, max));
    }

}
