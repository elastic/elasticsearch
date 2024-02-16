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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe.calculateIngestionLoadForExecutor;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.closeTo;

public class IngestLoadProbeTests extends ESTestCase {

    public void testCalculateIngestionLoadForExecutor() {
        final TimeValue maxTimeToClearQueue = TimeValue.timeValueSeconds(10);
        // Initially average task execution time is 0.0.
        double randomQueueContribution = randomDoubleBetween(0.1, 100.0, true);
        assertThat(calculateIngestionLoadForExecutor(0.0, 0.0, 0, maxTimeToClearQueue, randomQueueContribution), closeTo(0.0, 1e-3));
        // When there is nothing in the queue, we'd still want to keep up with average load
        assertThat(
            calculateIngestionLoadForExecutor(1.0, timeValueMillis(100).nanos(), 0, maxTimeToClearQueue, randomQueueContribution),
            closeTo(1.0, 1e-3)
        );
        // A threadpool of 2 with average task time of 100ms can run 200 tasks per 10 seconds.
        assertThat(
            calculateIngestionLoadForExecutor(
                0.0,
                timeValueMillis(100).nanos(),
                100,
                maxTimeToClearQueue,
                randomDoubleBetween(1.0, 100.0, true)
            ),
            closeTo(1.00, 1e-3)
        );
        // We have 1 task in the queue, we'd need roughly 1/100th of a thread more since each thread can do 100 tasks
        // per maxTimeToClearQueue period.
        assertThat(
            calculateIngestionLoadForExecutor(
                1.0,
                timeValueMillis(100).nanos(),
                1,
                maxTimeToClearQueue,
                randomDoubleBetween(0.01, 100, true)
            ),
            closeTo(1.01, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                1.0,
                timeValueMillis(100).nanos(),
                100,
                maxTimeToClearQueue,
                randomDoubleBetween(1.00, 100, true)
            ),
            closeTo(2.00, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                200,
                maxTimeToClearQueue,
                randomDoubleBetween(2.00, 100, true)
            ),
            closeTo(4.00, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                400,
                maxTimeToClearQueue,
                randomDoubleBetween(4.00, 100, true)
            ),
            closeTo(6.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                2.0,
                timeValueMillis(100).nanos(),
                1000,
                maxTimeToClearQueue,
                randomDoubleBetween(10.00, 100, true)
            ),
            closeTo(12.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 1000, maxTimeToClearQueue, 4.00),
            closeTo(6.0, 1e-3)
        );
    }

    public void testGetIngestionLoad() {
        Map<String, ExecutorStats> statsPerExecutor = new HashMap<>();
        var ingestLoadProbe = new IngestLoadProbe(
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, MAX_TIME_TO_CLEAR_QUEUE, MAX_QUEUE_CONTRIBUTION_FACTOR)
            ),
            statsPerExecutor::get
        );

        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), 0, between(0, 10)));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, between(0, 10)));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0, between(0, 10)));
        assertThat(ingestLoadProbe.getIngestionLoad(), closeTo(6.0, 1e-3));

        statsPerExecutor.clear();
        // With 200ms per task each thread can do 5 tasks per second
        var queueEmpty = randomBoolean();
        int queueSize = queueEmpty ? 0 : 5 * (int) MAX_TIME_TO_CLEAR_QUEUE.getDefault(Settings.EMPTY).seconds();
        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), queueSize, 1));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 1));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0, 1));
        var expectedExtraThreads = queueEmpty ? 0.0 : 1.0;
        assertThat(ingestLoadProbe.getIngestionLoad(), closeTo(6.0 + expectedExtraThreads, 1e-3));
    }
}
