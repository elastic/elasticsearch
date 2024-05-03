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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
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
            var searchLoadSampler = new AverageSearchLoadSampler(threadpool, timeValueSeconds(1), DEFAULT_EWMA_ALPHA, 4);
            searchLoadSampler.sample();
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH).threadsUsed(), equalTo(0.0));
            var randomThreadPool = randomValueOtherThanMany(
                AverageSearchLoadSampler.SEARCH_EXECUTORS::contains,
                () -> randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet())
            );
            expectThrows(IllegalArgumentException.class, () -> searchLoadSampler.getSearchExecutorStats(randomThreadPool).threadsUsed());

            var searchExecutor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.SEARCH);
            searchExecutor.execute(() -> safeSleep(randomLongBetween(50, 200)));
            assertBusy(() -> assertThat(searchExecutor.getCompletedTaskCount(), equalTo(1L)));
            searchLoadSampler.sample();
            assertThat(searchLoadSampler.getSearchExecutorStats(ThreadPool.Names.SEARCH).threadsUsed(), greaterThan(0.0));
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

    public void testContinuousFullLoad() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var cycles = 1000;
        for (int totalTime = executorMaxThreads; totalTime < cycles * executorMaxThreads; totalTime += executorMaxThreads) {
            averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of());
        }
        assertThat("The load should converge to the number of threads", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
    }

    public void testContinuousHalfLoad() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var cycles = 1000;
        var halfExecutorMaxThreads = executorMaxThreads / 2;
        for (int totalTime = halfExecutorMaxThreads; totalTime < cycles * halfExecutorMaxThreads; totalTime += halfExecutorMaxThreads) {
            averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of());
        }
        assertThat("The load should converge to half the size of the executor", averageLoad.get(), closeTo(halfExecutorMaxThreads, 1e-3));
    }

    public void testSamplingFrequency() {
        int executorMaxThreads = 4;
        double alpha = 0.2;

        for (int samplingInterval = 500; samplingInterval < 5_000; samplingInterval += 500) {
            TimeValue samplingFrequency = timeValueMillis(samplingInterval);
            var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

            var cycles = 1000;
            for (int totalTime = samplingInterval; totalTime < cycles * samplingInterval; totalTime += samplingInterval) {
                averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of());
            }
            assertThat("The load should converge to the number of threads", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
        }
    }

    public void testLoadWithHalfOngoingTasks() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var cycles = 1000;
        var halfExecutorMaxThreads = executorMaxThreads / 2;
        for (int totalTime = halfExecutorMaxThreads; totalTime < cycles * halfExecutorMaxThreads; totalTime += halfExecutorMaxThreads) {
            averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of(0L, 0L));
        }
        assertThat("The load should converge to the size of the executor", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
    }

    public void testThatLoadIsCapped() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var cycles = 1000;
        var twiceExecutorMaxThreads = executorMaxThreads * 2;
        for (int totalTime = twiceExecutorMaxThreads; totalTime < cycles * twiceExecutorMaxThreads; totalTime += twiceExecutorMaxThreads) {
            averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of(0L, 0L, 0L));
        }
        assertThat("The load should converge to the size of the executor", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
    }

    public void testLoadWithMixOfOngoingTasks() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        long totalExecutorTimeNanos = 0;
        for (var cycle = 0; cycle < 100; cycle++) {
            List<Long> ongoingTasksStartNanos = new ArrayList<>();
            if (cycle % 10 == 0) {
                // Every 10th measurement cycle, we reset to full load up to this point.
                totalExecutorTimeNanos = cycle * timeValueSeconds(executorMaxThreads).nanos();
            } else {
                // For other measurement cycles, we add a full load of ongoing tasks.
                for (int i = 0; i < executorMaxThreads; i++) {
                    ongoingTasksStartNanos.add(samplingFrequency.nanos());
                }
            }
            long nowNanos = cycle * samplingFrequency.nanos();
            averageLoad.update(totalExecutorTimeNanos, ongoingTasksStartNanos, nowNanos);
        }

        assertThat("The load should converge to the number of threads", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
    }

    public void testFullLoadFromOngoingTasks() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var numberOfOngoingTasks = executorMaxThreads;
        long totalExecutorTimeNanos = 0;
        for (var cycle = 0; cycle < 100; cycle++) {
            List<Long> ongoingTasksStartNanos = new ArrayList<>();
            // we add a full load of ongoing tasks.
            for (int i = 0; i < numberOfOngoingTasks; i++) {
                ongoingTasksStartNanos.add(samplingFrequency.nanos());
            }
            long nowNanos = cycle * samplingFrequency.nanos();
            averageLoad.update(totalExecutorTimeNanos, ongoingTasksStartNanos, nowNanos);
        }

        assertThat("The load should converge to the number of ongoing tasks", averageLoad.get(), closeTo(numberOfOngoingTasks, 1e-3));
    }

    public void testWithHalfLoadFromOngoingTasks() {
        int executorMaxThreads = 4;
        TimeValue samplingFrequency = timeValueSeconds(1);
        double alpha = 0.2;
        var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

        var numberOfOngoingTasks = executorMaxThreads / 2;
        for (var cycle = 0; cycle < 100; cycle++) {
            List<Long> ongoingTasksStartNanos = new ArrayList<>();
            // we add a half load of ongoing tasks.
            for (int i = 0; i < numberOfOngoingTasks; i++) {
                ongoingTasksStartNanos.add(samplingFrequency.nanos());
            }
            long nowNanos = cycle * samplingFrequency.nanos();
            averageLoad.update(0, ongoingTasksStartNanos, nowNanos);
        }

        assertThat("The load should converge to the number of ongoing tasks", averageLoad.get(), closeTo(numberOfOngoingTasks, 1e-3));
    }

    public void testLoadWithMultipleThreadPoolSizes() {
        for (int executorMaxThreads = 1; executorMaxThreads < 10; executorMaxThreads++) {
            TimeValue samplingFrequency = timeValueSeconds(1);
            double alpha = 0.2;
            var averageLoad = new AverageSearchLoadSampler.AverageLoad(executorMaxThreads, samplingFrequency, alpha);

            var cycles = 1000;
            for (int totalTime = executorMaxThreads; totalTime < cycles * executorMaxThreads; totalTime += executorMaxThreads) {
                averageLoad.update(timeValueSeconds(totalTime).nanos(), List.of());
            }
            assertThat("The load should converge to the number of threads", averageLoad.get(), closeTo(executorMaxThreads, 1e-3));
        }
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
