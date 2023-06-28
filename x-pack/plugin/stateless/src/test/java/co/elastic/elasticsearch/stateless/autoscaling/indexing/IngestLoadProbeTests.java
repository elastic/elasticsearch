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

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe.calculateIngestionLoadForExecutor;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.closeTo;

public class IngestLoadProbeTests extends ESTestCase {

    public void testCalculateIngestionLoadForExecutor() {
        assertThat(calculateIngestionLoadForExecutor(0.0, 0.0, 0, TimeValue.timeValueSeconds(10)), closeTo(0.0, 1e-3));
        assertThat(calculateIngestionLoadForExecutor(1.0, 0.0, 0, TimeValue.timeValueSeconds(10)), closeTo(1.0, 1e-3));
        // A threadpool of 2 with average task time of 100ms can run 200 tasks per 10 seconds.
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 100, TimeValue.timeValueSeconds(10)),
            closeTo(2.00, 1e-3)
        );
        // Up to 200 (based on maxTimeToClearQueue of 10 sec) is manageable by the current number of threads
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 200, TimeValue.timeValueSeconds(10)),
            closeTo(2.00, 1e-3)
        );
        // We have 1 task more than maxTimeToClearQueue, we'd need roughly 1/100th of a thread more since each thread can do 100 tasks
        // per maxTimeToClearQueue period.
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 201, TimeValue.timeValueSeconds(10)),
            closeTo(2.01, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 300, TimeValue.timeValueSeconds(10)),
            closeTo(3.0, 1e-3)
        );
        // We have twice as many tasks to handle as a maxTimeToClearQueue period, we'd need twice the threads
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 400, TimeValue.timeValueSeconds(10)),
            closeTo(4.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 1000, TimeValue.timeValueSeconds(10)),
            closeTo(10.0, 1e-3)
        );
        // When there is nothing in the queue, we'd still want to keep up with average load
        assertThat(
            calculateIngestionLoadForExecutor(2.0, timeValueMillis(100).nanos(), 0, TimeValue.timeValueSeconds(10)),
            closeTo(2.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(1.0, timeValueMillis(100).nanos(), 0, TimeValue.timeValueSeconds(10)),
            closeTo(1.0, 1e-3)
        );
    }

    public void testGetIngestionLoad() {
        Map<String, ExecutorStats> statsPerExecutor = new HashMap<>();
        var ingestLoadProbe = new IngestLoadProbe(statsPerExecutor::get);

        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), 0));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0));
        assertThat(ingestLoadProbe.getIngestionLoad(), closeTo(6.0, 1e-3));

        statsPerExecutor.clear();
        // With 200ms per task each thread can do 5 tasks per second
        // 3 is the existing average load (max threads busy)
        long queueSizeManageableWithinMaxTime = 5 * MAX_TIME_TO_CLEAR_QUEUE.seconds() * 3;
        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), (int) queueSizeManageableWithinMaxTime * 2));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0));
        assertThat(ingestLoadProbe.getIngestionLoad(), closeTo(6.0 + 3.0, 1e-3));
    }
}
