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

public class AverageWriteLoadSampler {

    static final Set<String> WRITE_EXECUTORS = Set.of(Names.WRITE, Names.SYSTEM_WRITE, Names.SYSTEM_CRITICAL_WRITE);

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
        WRITE_EXECUTORS.forEach(name -> averageLoadPerExector.put(name, new AverageLoad()));
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

    public double getAverageWriteLoad(String executor) {
        if (WRITE_EXECUTORS.contains(executor) == false) {
            throw new IllegalArgumentException("only the following write executors are valid: " + WRITE_EXECUTORS);
        }
        return averageLoadPerExector.get(executor).get();
    }

    /**
     * Represents the weighted average load of the past minute in terms of the average number of threads busy during the
     * last minute. Each sample (1 second interval) represents the proportion of time (wall clock) spent running tasks.
     * <p>
     * The EWMA alpha value (0.2) is chosen such that samples older than 1 minute have a negligible effect on the average load.
     */
    private class AverageLoad {
        private static final double EWMA_ALPHA = 0.2;

        private volatile long previousTotalExecutionTimeNanos = 0;
        private final ExponentiallyWeightedMovingAverage writeLoadEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0.0);

        public void update(long currentTotalExecutionTimeNanos) {
            final long previous = previousTotalExecutionTimeNanos;
            assert currentTotalExecutionTimeNanos >= previous;
            var taskExecutionTimeSinceLastSample = currentTotalExecutionTimeNanos - previous;
            writeLoadEWMA.addValue((double) taskExecutionTimeSinceLastSample / AverageWriteLoadSampler.this.samplingFrequency.nanos());
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
