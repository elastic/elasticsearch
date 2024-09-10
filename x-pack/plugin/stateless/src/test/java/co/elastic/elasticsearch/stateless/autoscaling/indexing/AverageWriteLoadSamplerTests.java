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

import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler.DEFAULT_EWMA_ALPHA;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler.ensureRange;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AverageWriteLoadSamplerTests extends ESTestCase {

    // TODO: could we make AverageWriteLoadSampler work with a DeterministicTaskQueue to be able to write more elaborate tests?
    public void testAverageWriteLoadInitialValue() throws Exception {
        var threadpool = new TestThreadPool("test");
        try {
            var writeLoadSampler = new AverageWriteLoadSampler(threadpool, timeValueSeconds(1), DEFAULT_EWMA_ALPHA, DEFAULT_EWMA_ALPHA);
            writeLoadSampler.sample();
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.WRITE).averageLoad(), equalTo(0.0));
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.SYSTEM_WRITE).averageLoad(), equalTo(0.0));
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.SYSTEM_CRITICAL_WRITE).averageLoad(), equalTo(0.0));
            var randomThreadPool = randomValueOtherThanMany(
                AverageWriteLoadSampler.WRITE_EXECUTORS::contains,
                () -> randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet())
            );
            expectThrows(IllegalArgumentException.class, () -> writeLoadSampler.getExecutorStats(randomThreadPool).averageLoad());

            var writeExecutor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.WRITE);
            writeExecutor.execute(() -> safeSleep(randomLongBetween(50, 200)));
            assertBusy(() -> assertThat(writeExecutor.getCompletedTaskCount(), equalTo(1L)));
            writeLoadSampler.sample();
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.WRITE).averageLoad(), greaterThan(0.0));
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.SYSTEM_WRITE).averageLoad(), equalTo(0.0));
            assertThat(writeLoadSampler.getExecutorStats(ThreadPool.Names.SYSTEM_CRITICAL_WRITE).averageLoad(), equalTo(0.0));
        } finally {
            terminate(threadpool);
        }
    }

    public void testAverageLoadUsesEWMA() {
        double alpha = 0.2;
        var averageLoad = new AverageWriteLoadSampler.AverageLoad(4, timeValueSeconds(1), alpha, alpha);
        averageLoad.update(0, List.of(), 0);
        assertThat(averageLoad.getWriteLoad(), equalTo(0.0));
        assertThat(averageLoad.getQueueSize(), equalTo(0.0));
        int currentQueueSize1 = between(0, 1000);
        averageLoad.update(timeValueSeconds(10).nanos(), List.of(), currentQueueSize1);
        assertThat(averageLoad.getWriteLoad(), closeTo(alpha * 4, 1e-3));
        double ewma1 = alpha * currentQueueSize1;
        assertThat(averageLoad.getQueueSize(), closeTo(ewma1, 1e-3));
        int currentQueueSize2 = between(0, 1000);
        averageLoad.update(timeValueSeconds(10).nanos(), List.of(), currentQueueSize2);
        assertThat(averageLoad.getQueueSize(), closeTo((alpha * currentQueueSize2) + (1 - alpha) * ewma1, 1e-3));
    }

    public void testEnsureRange() {
        int n = randomIntBetween(1, 1000);
        long max = randomLongBetween(1, Long.MAX_VALUE);
        for (int i = 0; i < n; i++) {
            assertThat(ensureRange(randomLong(), 0, max), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(max)));
        }
    }

    public void testCalculateTaskExecutionTimeSinceLastSample() {
        var samplingFrequency = timeValueSeconds(1);
        var maxThreadpoolSize = 3;
        var averageLoad = new AverageWriteLoadSampler.AverageLoad(maxThreadpoolSize, samplingFrequency, 0.2, 0.2);
        var nowNanos = System.nanoTime();

        var previous = randomLongBetween(0, timeValueSeconds(10).nanos());
        var current = previous;
        assertThat(
            averageLoad.calculateTaskExecutionTimeSinceLastSample(
                current,
                previous,
                List.of(nowNanos - timeValueMillis(100).nanos()), // A task that started 100ms ago
                nowNanos
            ),
            equalTo(timeValueMillis(100).nanos())
        );
        // Tasks longer than the sampling period get capped.
        previous = randomLongBetween(0, timeValueSeconds(10).nanos());
        current = previous;
        assertThat(
            averageLoad.calculateTaskExecutionTimeSinceLastSample(
                current,
                previous,
                List.of(
                    nowNanos - timeValueMillis(100).nanos(), // A task that started 100ms ago
                    nowNanos - timeValueSeconds(10).nanos() // A long running task
                ),
                nowNanos
            ),
            equalTo(timeValueMillis(1100).nanos())
        );
        // Task execution time since last sample cannot be more than the number of threads multiplied by sampling period.
        previous = randomLongBetween(0, timeValueSeconds(10).nanos());
        current = previous + randomLongBetween(0, maxThreadpoolSize * samplingFrequency.nanos());
        assertThat(
            averageLoad.calculateTaskExecutionTimeSinceLastSample(
                current,
                previous,
                List.of(
                    nowNanos - timeValueMillis(100).nanos(), // A task that started 100ms ago
                    nowNanos - timeValueSeconds(10).nanos(), // A long running task
                    nowNanos - timeValueSeconds(10).nanos(), // A long running task
                    nowNanos - timeValueSeconds(10).nanos() // A long running task
                ),
                nowNanos
            ),
            equalTo(maxThreadpoolSize * samplingFrequency.nanos())
        );
        // A task that finishes right when the sampling happens could be double counted.
        previous = randomLongBetween(0, timeValueSeconds(10).nanos());
        var taskDuration = timeValueSeconds(2);
        current = previous + taskDuration.nanos();  // The task just finished and is included in the current total execution time
        assertThat(
            averageLoad.calculateTaskExecutionTimeSinceLastSample(
                current,
                previous,
                List.of(nowNanos - taskDuration.nanos()), // But it is not yet removed from the ongoing tasks
                nowNanos
            ),
            // taskExecutionTimeSinceLastSample has over-counted the task, but still capped to max possible within the sampling period.
            allOf(greaterThan(taskDuration.nanos()), lessThanOrEqualTo(maxThreadpoolSize * samplingFrequency.nanos()))
        );
    }
}
