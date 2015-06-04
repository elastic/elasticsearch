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
package org.elasticsearch.snapshots;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 */
@Ignore
public abstract class AbstractSnapshotTests extends ElasticsearchIntegrationTest {

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static int numberOfFiles(Path dir) throws IOException {
        final AtomicInteger count = new AtomicInteger();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                count.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });
        return count.get();
    }

    public static void stopNode(final String node) throws IOException {
        internalCluster().stopRandomNode(new Predicate<Settings>() {
            @Override
            public boolean apply(Settings settings) {
                return settings.get("name").equals(node);
            }
        });
    }

    public void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshot, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        SnapshotId snapshotId = new SnapshotId(repository, snapshot);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            List<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot).get().getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
                SnapshotMetaData snapshotMetaData = stateResponse.getState().getMetaData().custom(SnapshotMetaData.TYPE);
                if (snapshotMetaData == null || snapshotMetaData.snapshot(snapshotId) == null) {
                    return snapshotInfos.get(0);
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public static String blockNodeWithIndex(String index) {
        for(String node : internalCluster().nodesInclude("test-idx")) {
            ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository("test-repo")).blockOnDataFiles(true);
            return node;
        }
        fail("No nodes for the index " + index + " found");
        return null;
    }

    public static void unblockNode(String node) {
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository("test-repo")).unblock();
    }

    protected void assertBusyPendingTasks(final String taskPrefix, final int expectedCount) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                PendingClusterTasksResponse tasks = client().admin().cluster().preparePendingClusterTasks().get();
                int count = 0;
                for(PendingClusterTask task : tasks) {
                    if (task.getSource().toString().startsWith(taskPrefix)) {
                        count++;
                    }
                }
                assertThat(count, greaterThanOrEqualTo(expectedCount));
            }
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Cluster state task that blocks waits for the blockOn task to show up and then blocks execution not letting
     * any cluster state update task to be performed unless they have priority higher then passThroughPriority.
     *
     * This class is useful to testing of cluster state update task batching for lower priority tasks.
     */
    protected class BlockingClusterStateListener implements ClusterStateListener {

        private final Predicate<ClusterChangedEvent> blockOn;
        private final Predicate<ClusterChangedEvent> countOn;
        private final ClusterService clusterService;
        private final CountDownLatch latch;
        private final Priority passThroughPriority;
        private int count;
        private boolean timedOut;
        private final TimeValue timeout;
        private long stopWaitingAt = -1;

        public BlockingClusterStateListener(ClusterService clusterService, String blockOn, String countOn, Priority passThroughPriority) {
            this(clusterService, blockOn, countOn, passThroughPriority, TimeValue.timeValueMinutes(1));
        }

        public BlockingClusterStateListener(ClusterService clusterService, final String blockOn, final String countOn, Priority passThroughPriority, TimeValue timeout) {
            this.clusterService = clusterService;
            this.blockOn = new Predicate<ClusterChangedEvent>() {
                @Override
                public boolean apply(ClusterChangedEvent clusterChangedEvent) {
                    return clusterChangedEvent.source().startsWith(blockOn);
                }
            };
            this.countOn = new Predicate<ClusterChangedEvent>() {
                @Override
                public boolean apply(ClusterChangedEvent clusterChangedEvent) {
                    return clusterChangedEvent.source().startsWith(countOn);
                }
            };
            this.latch = new CountDownLatch(1);
            this.passThroughPriority = passThroughPriority;
            this.timeout = timeout;

        }

        public void unblock() {
            latch.countDown();
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (blockOn.apply(event)) {
                logger.info("blocking cluster state tasks on [{}]", event.source());
                assert stopWaitingAt < 0; // Make sure we are the first time here
                stopWaitingAt = System.currentTimeMillis() + timeout.getMillis();
                addBlock();
            }
            if (countOn.apply(event)) {
                count++;
            }
        }

        private void addBlock() {
            // We should block after this task - add blocking cluster state update task
            clusterService.submitStateUpdateTask("test_block", passThroughPriority, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    while(System.currentTimeMillis() < stopWaitingAt) {
                        for (PendingClusterTask task : clusterService.pendingTasks()) {
                            if (task.getSource().string().equals("test_block") == false && passThroughPriority.sameOrAfter(task.getPriority())) {
                                // There are other higher priority tasks in the queue and let them pass through and then set the block again
                                logger.info("passing through cluster state task {}", task.getSource());
                                addBlock();
                                return currentState;
                            }
                        }
                        try {
                            logger.info("waiting....");
                            if (latch.await(Math.min(100, timeout.millis()), TimeUnit.MILLISECONDS)){
                                // Done waiting - unblock
                                logger.info("unblocked");
                                return currentState;
                            }
                            logger.info("done waiting....");
                        } catch (InterruptedException ex) {
                            logger.info("interrupted....");
                            Thread.currentThread().interrupt();
                            return currentState;
                        }
                    }
                    timedOut = true;
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to execute [{}]", t, source);
                }
            });

        }

        public int count() {
            return count;
        }

        public boolean timedOut() {
            return timedOut;
        }
    }
}
