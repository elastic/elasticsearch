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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR;
import static co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE;
import static co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadProbe.calculateSearchLoadForExecutor;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.threadpool.ThreadPool.searchOrGetThreadPoolSize;
import static org.hamcrest.Matchers.closeTo;

public class SearchLoadProbeTests extends ESTestCase {

    public void testCalculateSearchLoadForExecutor() {
        final TimeValue maxTimeToClearQueue = TimeValue.timeValueSeconds(10);
        // Initially average task execution time is 0.0.
        double randomQueueContribution = randomDoubleBetween(0.1, 100.0, true);
        assertThat(calculateSearchLoadForExecutor(0.0, 0.0, 0, maxTimeToClearQueue, randomQueueContribution, 2, 2), closeTo(0.0, 1e-3));
        // We should return the search load for just the thread pool when the maxTimeToClearQueue is 0.
        assertThat(
            calculateSearchLoadForExecutor(1.0, 1.0, 1, TimeValue.timeValueSeconds(0), randomQueueContribution, 2, 2),
            closeTo(1.0, 1e-3)
        );
        // When there is nothing in the queue, we'd still want to keep up with average load
        assertThat(
            calculateSearchLoadForExecutor(1.0, timeValueMillis(100).nanos(), 0, maxTimeToClearQueue, randomQueueContribution, 2, 2),
            closeTo(1.0, 1e-3)
        );
        // A threadpool of 2 with average task time of 100ms can run 200 tasks per 10 seconds.
        assertThat(
            calculateSearchLoadForExecutor(
                0.0,
                timeValueMillis(100).nanos(),
                100,
                maxTimeToClearQueue,
                randomDoubleBetween(1.0, 100.0, true),
                2,
                2
            ),
            closeTo(1.00, 1e-3)
        );
        // We have 1 task in the queue, we'd need roughly 1/100th of a thread more since each thread can do 100 tasks
        // per maxTimeToClearQueue period.
        assertThat(
            calculateSearchLoadForExecutor(
                1.0,
                timeValueMillis(100).nanos(),
                1,
                maxTimeToClearQueue,
                randomDoubleBetween(0.01, 100, true),
                2,
                2
            ),
            closeTo(1.01, 1e-3)
        );
        assertThat(
            calculateSearchLoadForExecutor(
                1.0,
                timeValueMillis(100).nanos(),
                100,
                maxTimeToClearQueue,
                randomDoubleBetween(1.00, 100, true),
                2,
                2
            ),
            closeTo(2.00, 1e-3)
        );
        assertThat(
            calculateSearchLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                200,
                maxTimeToClearQueue,
                randomDoubleBetween(2.00, 100, true),
                2,
                2
            ),
            closeTo(4.00, 1e-3)
        );
        assertThat(
            calculateSearchLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                400,
                maxTimeToClearQueue,
                randomDoubleBetween(4.00, 100, true),
                2,
                2
            ),
            closeTo(6.0, 1e-3)
        );
        assertThat(
            calculateSearchLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                1000,
                maxTimeToClearQueue,
                randomDoubleBetween(10.00, 100, true),
                2,
                2
            ),
            closeTo(12.0, 1e-3)
        );
        assertThat(
            calculateSearchLoadForExecutor(2.0, timeValueMillis(100).nanos(), 1000, maxTimeToClearQueue, 4.00, 2, 2),
            closeTo(6.0, 1e-3)
        );
    }

    public void testGetSearchLoad() {
        Map<String, SearchExecutorStats> statsPerExecutor = new HashMap<>();
        var searchLoadProbe = new SearchLoadProbe(
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, MAX_TIME_TO_CLEAR_QUEUE, MAX_QUEUE_CONTRIBUTION_FACTOR)
            ),
            statsPerExecutor::get
        );

        statsPerExecutor.put(Names.SEARCH, new SearchExecutorStats(3.0, timeValueMillis(200).nanos(), 0, 10, 10));
        assertThat(searchLoadProbe.getSearchLoad(), closeTo(3.0, 1e-3));

        statsPerExecutor.clear();
        statsPerExecutor.put(Names.SEARCH, new SearchExecutorStats(1.0, timeValueMillis(200).nanos(), 0, 1, 1));
        assertThat(searchLoadProbe.getSearchLoad(), closeTo(1.0, 1e-3));

        statsPerExecutor.clear();
        statsPerExecutor.put(Names.SEARCH, new SearchExecutorStats(1.0, timeValueMillis(200).nanos(), 0, 1, 2));
        assertThat(searchLoadProbe.getSearchLoad(), closeTo(2.0, 1e-3));

        statsPerExecutor.clear();
        // With 200ms per task each thread can do 5 tasks per second
        int queueSize = 5 * (int) MAX_TIME_TO_CLEAR_QUEUE.getDefault(Settings.EMPTY).seconds();
        double threadsUsed = 2.0;
        int maxThreads = 2;
        int numProcessors = 4;
        statsPerExecutor.put(
            Names.SEARCH,
            new SearchExecutorStats(threadsUsed, timeValueMillis(200).nanos(), queueSize, maxThreads, numProcessors)
        );
        var expectedExtraThreads = threadsUsed * ((double) numProcessors / maxThreads);
        assertThat(searchLoadProbe.getSearchLoad(), closeTo(threadsUsed + expectedExtraThreads, 1e-3));
    }

    public void testSearchLoadJustThreadPool() {
        assertSearchLoadNoQueuing(4, 7, 4.0);
        assertSearchLoadNoQueuing(4, 0, 0.0);

        // Check the boundaries of the acceptable ranges of inputs for edge cases:
        assertSearchLoadNoQueuing(1, 0, 0.0);
        assertSearchLoadNoQueuing(1, 1, 0.5);
        assertSearchLoadNoQueuing(1, 2, 1.0);
        assertSearchLoadNoQueuing(2, 0, 0.0);
        assertSearchLoadNoQueuing(2, 1, 0.5);
        assertSearchLoadNoQueuing(2, 2, 1.0);
        assertSearchLoadNoQueuing(2, 0.001, 0.0005);
    }

    public void testSearchLoadJustQueuing() {
        // Search Load should scale linearly with the queue size:
        assertSearchJustQueuing(4, 10, 10, 7, 4.0);
        assertSearchJustQueuing(4, 10, 10, 14, 8.0);
        assertSearchJustQueuing(4, 10, 10, 21, 12.0);

        // The load should be inversely proportional to the maxTimeToClearQueue setting.
        assertSearchJustQueuing(4, 20, 10, 7, 2.0);
        assertSearchJustQueuing(4, 5, 10, 7, 8.0);

        // The load should be proportional to the measured execution time (TaskTimeMillis).
        assertSearchJustQueuing(4, 10, 5, 7, 2.0);
        assertSearchJustQueuing(4, 10, 20, 7, 8.0);

        // We should have no search load reported when the queues are zero.
        assertSearchJustQueuing(1, 10, 0, 0, 0.0);
        assertSearchJustQueuing(2, 10, 0, 0, 0.0);
        assertSearchJustQueuing(3, 10, 0, 0, 0.0);
    }

    public void testSearchLoadThreadPoolAndQueuing() {
        // The Search executor load should be the sum of the pool and queue loads.
        assertSearchLoad(4, 10, 7, 10, 7, 8.0);
        assertSearchLoad(4, 10, 14, 10, 7, 12.0);
        assertSearchLoad(4, 10, 7, 10, 14, 12.0);
        assertSearchLoad(4, 10, 14, 10, 14, 16.0);
    }

    public void testMaxTimeToClearQueueSetting() {}

    private void assertSearchLoadNoQueuing(int numProcessors, double searchPoolThreadsUsed, double expectedTotalReportedLoad) {
        assertSearchLoad(numProcessors, 1000, searchPoolThreadsUsed, 100, 0, expectedTotalReportedLoad);
    }

    private void assertSearchJustQueuing(
        int numProcessors,
        int maxTimeToClearQueueMillis,
        int searchTaskTimeMillis,
        int searchQueueSize,
        double expectedTotalReportedLoad
    ) {
        assertSearchLoad(numProcessors, maxTimeToClearQueueMillis, 0, searchTaskTimeMillis, searchQueueSize, expectedTotalReportedLoad);
    }

    private void assertSearchLoad(
        int numProcessors,
        int maxTimeToClearQueueMillis,
        double searchPoolThreadsUsed,
        int searchTaskTimeMillis,
        int searchQueueSize,
        double expectedTotalReportedLoad
    ) {
        Map<String, SearchExecutorStats> statsPerExecutor = new HashMap<>();
        var searchLoadProbe = new SearchLoadProbe(
            new ClusterSettings(
                Settings.builder().put(MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueMillis(maxTimeToClearQueueMillis)).build(),
                Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, MAX_TIME_TO_CLEAR_QUEUE, MAX_QUEUE_CONTRIBUTION_FACTOR)
            ),
            statsPerExecutor::get
        );

        statsPerExecutor.put(
            Names.SEARCH,
            new SearchExecutorStats(
                searchPoolThreadsUsed,
                timeValueMillis(searchTaskTimeMillis).nanos(),
                searchQueueSize,
                searchOrGetThreadPoolSize(numProcessors),
                numProcessors
            )
        );

        assertThat(searchLoadProbe.getSearchLoad(), closeTo(expectedTotalReportedLoad, 1e-3));
    }
}
