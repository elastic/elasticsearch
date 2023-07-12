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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler.DEFAULT_EWMA_ALPHA;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;

public class AverageWriteLoadSamplerTests extends ESTestCase {

    // TODO: could we make AverageWriteLoadSampler work with a DeterministicTaskQueue to be able to write more elaborate tests?
    public void testAverageWriteLoadInitialValue() throws Exception {
        var threadpool = new TestThreadPool("test");
        try {
            var writeLoadSampler = new AverageWriteLoadSampler(threadpool, TimeValue.timeValueSeconds(1), DEFAULT_EWMA_ALPHA);
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

    public void testWriteLoadEWMAUpdateIsCapped() {
        double alpha = 0.2;
        var averageLoad = new AverageWriteLoadSampler.AverageLoad(4, TimeValue.timeValueSeconds(1), alpha);
        averageLoad.update(0);
        assertThat(averageLoad.get(), equalTo(0.0));
        // Cannot update more than 4 seconds at a time.
        averageLoad.update(TimeValue.timeValueSeconds(10).nanos());
        assertThat(averageLoad.get(), closeTo(alpha * 4, 1e-3));
    }
}
