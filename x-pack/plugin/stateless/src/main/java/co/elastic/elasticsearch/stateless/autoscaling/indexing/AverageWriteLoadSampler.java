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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// TODO: Use the same scheduled task for IngestLoadSampler and AverageWriteLoadSampler
public class AverageWriteLoadSampler {

    static final Set<String> WRITE_EXECUTORS = Set.of(Names.WRITE, Names.SYSTEM_WRITE, Names.SYSTEM_CRITICAL_WRITE);
    private static final double EWMA_ALPHA = 0.2;

    private final ThreadPool threadPool;
    private final TimeValue samplingFrequency;
    private final Map<String, AverageLoad> averageLoadPerExector = new HashMap<>();
    private volatile Scheduler.Cancellable cancellable;

    public static AverageWriteLoadSampler create(ThreadPool threadPool, Settings settings) {
        assert WRITE_EXECUTORS.stream()
            .map(threadPool::executor)
            .allMatch(executor -> executor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor);
        return new AverageWriteLoadSampler(threadPool, IngestLoadSampler.SAMPLING_FREQUENCY_SETTING.get(settings));
    }

    AverageWriteLoadSampler(ThreadPool threadPool, TimeValue samplingFrequency) {
        this.threadPool = threadPool;
        this.samplingFrequency = samplingFrequency;
        WRITE_EXECUTORS.forEach(
            name -> averageLoadPerExector.put(name, new AverageLoad(threadPool.info(name).getMax(), samplingFrequency, EWMA_ALPHA))
        );
    }

    public void start() {
        assert Objects.isNull(cancellable);
        cancellable = threadPool.scheduleWithFixedDelay(this::sampleAverageLoad, samplingFrequency, Names.GENERIC);
    }

    public void stop() {
        assert Objects.nonNull(cancellable);
        cancellable.cancel();
        cancellable = null;
    }

    private void sampleAverageLoad() {
        for (var name : WRITE_EXECUTORS) {
            long currentTotalNanos = ((TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(name))
                .getTotalTaskExecutionTime();
            averageLoadPerExector.get(name).update(currentTotalNanos);
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
            executor.getCurrentQueueSize()
        );
    }

    /**
     * Represents the average number of threads busy per second for an executor.
     * Each sample (1 second interval) represents the proportion of time (wall clock) spent running tasks. We use a weighted average and
     * the EWMA alpha value (0.2) is chosen such that samples older than 1 minute have a negligible effect on the average load.
     */
    static class AverageLoad {
        private volatile long previousTotalExecutionTimeNanos = 0;
        private final ExponentiallyWeightedMovingAverage writeLoadEWMA;
        private final long maxExecutionTimePerSamplingPeriodNanos;
        private final TimeValue samplingFrequency;

        AverageLoad(int executorMaxSize, TimeValue samplingFrequency, double EWMAAlpha) {
            this.maxExecutionTimePerSamplingPeriodNanos = executorMaxSize * samplingFrequency.nanos();
            this.samplingFrequency = samplingFrequency;
            this.writeLoadEWMA = new ExponentiallyWeightedMovingAverage(EWMAAlpha, 0.0);
        }

        public void update(long currentTotalExecutionTimeNanos) {
            final long previous = previousTotalExecutionTimeNanos;
            assert currentTotalExecutionTimeNanos >= previous;
            // cap the value that is added to the EWMA to max time possible within a sampling period
            // to avoid skewing the average due to very long tasks. This would is a workaround and ES-6391 should provide
            // a better solution to this.
            var taskExecutionTimeSinceLastSample = Math.min(
                currentTotalExecutionTimeNanos - previous,
                maxExecutionTimePerSamplingPeriodNanos
            );
            writeLoadEWMA.addValue((double) taskExecutionTimeSinceLastSample / samplingFrequency.nanos());
            previousTotalExecutionTimeNanos = currentTotalExecutionTimeNanos;
        }

        /**
         * Returns the average number of threads busy within the past 1 minute.
         * E.g. 2.0 means roughly 2 threads were busy within the past minute (including IO time).
         */
        public double get() {
            return writeLoadEWMA.getAverage();
        }
    }

}
