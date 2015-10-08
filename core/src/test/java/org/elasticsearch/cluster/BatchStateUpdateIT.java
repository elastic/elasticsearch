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
package org.elasticsearch.cluster;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class BatchStateUpdateIT extends ESIntegTestCase {

    @Test
    public void singleBatchGroupTest() throws Exception {
        String node = internalCluster().startNode();
        ClusterService clusterService = internalCluster().clusterService(node);
        final int tasks = randomIntBetween(10, 100);
        final CountDownLatch executeBlock = new CountDownLatch(1);
        final AtomicInteger taskCount = new AtomicInteger();
        ClusterStateUpdateTask<String> batchUpdateTask = new ClusterStateUpdateTask<String>() {
            @Override
            public ClusterState execute(ClusterState clusterState, Collection<String> drainedRequests) {
                for (String request : drainedRequests) {
                    logger.debug("Processed {}", request);
                }
                assertTrue(taskCount.addAndGet(drainedRequests.size()) <= tasks);
                if (taskCount.get() == tasks) {
                    executeBlock.countDown();
                }
                return clusterState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("batch update task failed:", t);
            }
        };

        final CountDownLatch block = new CountDownLatch(1);
        clusterService.addFirst(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                try {
                    block.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        clusterService.submitStateUpdateTask("blocker", new NoopClusterStateUpdateTask());

        for (int i = 0; i < tasks; i++) {
            clusterService.submitStateUpdateTask("task " + i, batchUpdateTask, "task_param" + i);
        }

        block.countDown();
        assertTrue(executeBlock.await(10, TimeUnit.SECONDS));
    }

    public static class NoopClusterStateUpdateTask extends ClusterStateUpdateTask<Void> {
        @Override
        public ClusterState execute(ClusterState currentState, Collection<Void> params) throws Exception {
            return currentState;
        }

        @Override
        public void onFailure(String source, Throwable t) {

        }
    }

    @Test
    public void handlingBatchFailureTest() throws Exception {
        String node = internalCluster().startNode();
        ClusterService clusterService = internalCluster().clusterService(node);
        final int firstBatchTasks = randomIntBetween(10, 100);
        final int secondBatchTasks = randomIntBetween(10, 100);
        final CountDownLatch secondBatchBlock = new CountDownLatch(secondBatchTasks);
        final CountDownLatch firstBatchBlock = new CountDownLatch(1);
        final CountDownLatch flushBlock = new CountDownLatch(1);
        final AtomicInteger firstBatchTaskCount = new AtomicInteger();
        final AtomicInteger secondBatchTaskCount = new AtomicInteger();
        final AtomicBoolean failureWasThrown = new AtomicBoolean();

        logger.info("First batch {}, second batch {}", firstBatchTasks, secondBatchTasks);
        ClusterStateUpdateTask<String> batchUpdateTask =  new ClusterStateUpdateTask<String>() {

            @Override
            public ClusterState execute(ClusterState clusterState, Collection<String> drainedRequests) {
                // We are failing the first batch
                if (drainedRequests.iterator().next().startsWith("1/")) {
                    if (firstBatchTaskCount.addAndGet(drainedRequests.size()) == firstBatchTasks) {
                        firstBatchBlock.countDown();
                    }
                    throw new RuntimeException("Fail the first batch task");
                }
                // We process the second batch and flush request
                for (String request : drainedRequests) {
                    logger.info("Processing {}", request);
                    if (request.equals("flush")) {
                        flushBlock.countDown();
                    } else {
                        // It must be batch 2 request
                        assertTrue(request, request.startsWith("2/"));
                        secondBatchTaskCount.incrementAndGet();
                        secondBatchBlock.countDown();
                    }
                }
                return clusterState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("batch update task failed:", t);
                failureWasThrown.set(true);
            }
        };

        CountDownLatch block = new CountDownLatch(1);
        clusterService.addFirst(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                try {
                    block.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });


        clusterService.submitStateUpdateTask("blocker", new NoopClusterStateUpdateTask());

        // Submitting the first batch - it should fail completly
        for (int i = 0; i < firstBatchTasks; i++) {
            clusterService.submitStateUpdateTask("first batch", batchUpdateTask, "1/" + i);
        }
        block.countDown();
        // Submitting the second batch - it should work fine
        assertTrue(firstBatchBlock.await(10, TimeUnit.SECONDS));
        for (int i = 0; i < secondBatchTasks; i++) {
            clusterService.submitStateUpdateTask("second batch", batchUpdateTask, "2/" + i);
        }
        assertTrue(secondBatchBlock.await(10, TimeUnit.SECONDS));
        // Submitting the flush task to make sure we processed everything from the second batch and counted all tasks
        clusterService.submitStateUpdateTask("flush task", batchUpdateTask, "flush");
        assertTrue(flushBlock.await(10, TimeUnit.SECONDS));
        assertEquals(secondBatchTasks, secondBatchTaskCount.get());
    }
}