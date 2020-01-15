/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.bulk;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class BulkRejectionSingleNodeIT extends ESSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put("thread_pool.write.queue_size", randomIntBetween(2, 20))
            .build();
    }

    private class NoOverflowCountDownLatch {
        private CountDownLatch latch = new CountDownLatch(1);
        private AtomicInteger counter;

        private NoOverflowCountDownLatch(int count) {
            this.counter = new AtomicInteger(count);
        }

        public void countDown() {
            int value = counter.decrementAndGet();
            assertThat(value, greaterThanOrEqualTo(0));
            if (value == 0) {
                latch.countDown();
            }
        }

        public void await(TimeValue waitTime) throws InterruptedException {
            assertTrue(latch.await(waitTime.millis(), TimeUnit.MILLISECONDS));
        }
    }

    public void testBulkRejectionOnWaitingForClusterStateUpdate() throws Exception {
        final String index = "test";
        assertAcked(client().admin().indices().prepareCreate(index));
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        ThreadPool.Info info = threadPool.info(ThreadPool.Names.WRITE);
        int maxActive = Math.toIntExact(info.getMax() + info.getQueueSize().getSingles());
        int requests = maxActive + randomIntBetween(1, 100);
        logger.info("maxActive {}, requests {}", maxActive, requests);
        NoOverflowCountDownLatch completed = new NoOverflowCountDownLatch(requests);
        NoOverflowCountDownLatch rejected = new NoOverflowCountDownLatch(requests - maxActive);
        CountDownLatch masterWaiting = new CountDownLatch(1);
        CountDownLatch releaseMaster = new CountDownLatch(1);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                masterWaiting.countDown();
                assertTrue(releaseMaster.await(10, TimeUnit.SECONDS));
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail();
            }
        });
        try {
            ActionListener<BulkResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    completed.countDown();
                    Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed)
                        .forEach(r -> {
                            assertThat(ExceptionsHelper.unwrapCause(r.getFailure().getCause()),
                                Matchers.instanceOf(EsRejectedExecutionException.class));
                            rejected.countDown();
                        });
                }

                @Override
                public void onFailure(Exception e) {
                    completed.countDown();
                    assertThat(ExceptionsHelper.unwrapCause(e), Matchers.instanceOf(EsRejectedExecutionException.class));
                    rejected.countDown();
                }
            };
            for (int i = 0; i < requests; ++i) {
                final BulkRequest request = new BulkRequest();
                request.add(new IndexRequest(index).source(Collections.singletonMap("key", "valuea" + i)));
                waitEmpty(threadPool, ThreadPool.Names.WRITE);
                client().bulk(request, listener);
            }

            rejected.await(TimeValue.timeValueSeconds(10));
        } finally {
            releaseMaster.countDown();
        }
        completed.await(TimeValue.timeValueSeconds(10));
    }

    private void waitEmpty(ThreadPool threadPool, String name) throws InterruptedException {
        long begin = System.currentTimeMillis();
        long sleep = 0;
        while (true) {
            ThreadPoolStats stats = threadPool.stats();
            if (StreamSupport.stream(stats.spliterator(), false).filter(s -> s.getName().equals(name))
                .anyMatch(s -> s.getQueue() == 0)) {
                return;
            }
            if (System.currentTimeMillis() > (begin + 10000)) {
                fail("Waiting for empty queue timed out: " + name);
            }
            Thread.sleep(sleep);
            sleep += 10;
        }
    }
}
