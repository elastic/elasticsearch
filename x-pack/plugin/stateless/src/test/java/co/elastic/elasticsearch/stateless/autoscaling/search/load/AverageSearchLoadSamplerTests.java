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

import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

import static co.elastic.elasticsearch.stateless.autoscaling.search.load.AverageSearchLoadSampler.DEFAULT_EWMA_ALPHA;
import static co.elastic.elasticsearch.stateless.autoscaling.search.load.AverageSearchLoadSampler.ensureRange;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AverageSearchLoadSamplerTests extends ESTestCase {

    public void testAverageSearchLoadInitialValue() throws Exception {
        var threadpool = new TestThreadPool("test");
        try {
            var searchLoadSampler = new AverageSearchLoadSampler(threadpool, timeValueSeconds(1), DEFAULT_EWMA_ALPHA);
            searchLoadSampler.sample();
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH).averageLoad(), equalTo(0.0));
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH_COORDINATION).averageLoad(), equalTo(0.0));
            var randomThreadPool = randomValueOtherThanMany(
                AverageSearchLoadSampler.SEARCH_EXECUTORS::contains,
                () -> randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet())
            );
            expectThrows(IllegalArgumentException.class, () -> searchLoadSampler.getSearchExecutorStats(randomThreadPool).averageLoad());

            var searchExecutor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.SEARCH);
            searchExecutor.execute(() -> safeSleep(randomLongBetween(50, 200)));
            assertBusy(() -> assertThat(searchExecutor.getCompletedTaskCount(), equalTo(1L)));
            searchLoadSampler.sample();
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH).averageLoad(), greaterThan(0.0));
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH_COORDINATION).averageLoad(), equalTo(0.0));
        } finally {
            terminate(threadpool);
        }
    }

    public void testSearchLoadEWMAUpdateIsCapped() {
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(4, timeValueSeconds(1), alpha);
        averageLoad.update(0, List.of());
        assertThat(averageLoad.get(), equalTo(0.0));
        // Cannot update more than 4 seconds at a time.
        averageLoad.update(timeValueSeconds(10).nanos(), List.of());
        assertThat(averageLoad.get(), closeTo(alpha * 4, 1e-3));
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
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(maxThreadpoolSize, samplingFrequency, 0.2);
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
