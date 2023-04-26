/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat.SNAPSHOT_ONLY_FORMAT_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class SnapshotStressTestsIT extends AbstractSnapshotIntegTestCase {

    public void testRandomSuccessfulActivities() throws InterruptedException {
        runTrackedCluster(true);
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    public void testRandomActivitiesWithFailures() throws InterruptedException {
        // Run once without the permits system. This can have failed operations.
        runTrackedCluster(false);
        // Run once with the permits system. This should have only successful operations.
        runTrackedCluster(true);
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    private static void runTrackedCluster(boolean mustSucceed) throws InterruptedException {
        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        new TrackedCluster(
            internalCluster(),
            nodeNames(discoveryNodes.getMasterNodes()),
            nodeNames(discoveryNodes.getDataNodes()),
            mustSucceed
        ).run();
    }

    private static Set<String> nodeNames(Map<String, DiscoveryNode> nodesMap) {
        return nodesMap.values().stream().map(DiscoveryNode::getName).collect(Collectors.toSet());
    }

    /**
     * Test harness for snapshot stress tests.
     *
     * The test performs random operations on the cluster, as if from an external client:
     *
     * - indexing docs, deleting and re-creating the indices
     * - restarting nodes
     * - removing and adding repositories
     * - taking snapshots (sometimes partial), cloning them, and deleting them
     *
     * It ensures that these operations should succeed via a system of shared/exclusive locks implemented via permits: acquiring a single
     * permit is a shared lock, whereas acquiring all the permits is an exclusive lock. So for instance taking a snapshot acquires a shared
     * lock on the repository (permitting other concurrent snapshots/clones/deletes) whereas deleting and recreating the repository requires
     * an exclusive lock (ensuring that there are no ongoing operations on the repository, and preventing any new such operations from
     * starting).
     *
     * None of the operations block. If the necessary locks aren't all available then the operation just releases the ones it has acquired
     * and tries again later.
     *
     * The test completes after completing a certain number of snapshots (see {@link #completedSnapshotLatch}) or after a certain time has
     * elapsed.
     *
     * There is a flag that can be turned off to disable the permits system. In this state, the operations simply happen, disregarding
     * other potentially concurrent operations, and can fail. Failures are ignored. The flag is intended to let the cluster operate in a
     * random manner, with many failures, and later ensure afterwards that the cluster is still operational and stable.
     */
    private static class TrackedCluster {

        static final Logger logger = LogManager.getLogger(TrackedCluster.class);
        static final String CLIENT = "client";

        private final ThreadPool threadPool = new TestThreadPool(
            "TrackedCluster",
            // a single thread for "client" activities, to limit the number of activities all starting at once
            new ScalingExecutorBuilder(CLIENT, 1, 1, TimeValue.ZERO, true, CLIENT)
        );

        private final AtomicBoolean shouldStop = new AtomicBoolean();
        private final InternalTestCluster cluster;
        private final Map<String, TrackedNode> nodes = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedRepository> repositories = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedIndex> indices = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedSnapshot> snapshots = ConcurrentCollections.newConcurrentMap();

        /**
         * If we acquire permits on nodes in a completely random order then we tend to block all possible restarts. Instead we always try
         * the nodes in the same order, held in this field, so that nodes nearer the end of the list are more likely to be restartable.
         * The elected master node is usually last in this list.
         */
        private volatile List<TrackedNode> shuffledNodes;

        private final AtomicInteger snapshotCounter = new AtomicInteger();
        private final CountDownLatch completedSnapshotLatch;

        /**
         * This flag regulates the applicability of the permits system.
         */
        private final boolean mustSucceed;

        /**
         * Encapsulates a common pattern of trying to acquire a bunch of resources and then transferring ownership elsewhere on success,
         * but releasing them on failure.
         */
        private class TransferableReleasables implements Releasable {

            private boolean transferred = false;
            private final List<Releasable> releasables = new ArrayList<>();

            <T extends Releasable> T add(T releasable) {
                if (mustSucceed) {
                    assert transferred == false : "already transferred";
                    releasables.add(releasable);
                }
                return releasable;
            }

            Releasable transfer() {
                if (mustSucceed) {
                    assert transferred == false : "already transferred";
                    transferred = true;
                    Collections.reverse(releasables);
                }
                return () -> Releasables.close(releasables);
            }

            @Override
            public void close() {
                if (transferred == false) {
                    Releasables.close(releasables);
                }
            }
        }

        @Nullable // if no permit was acquired
        private Releasable tryAcquirePermit(Semaphore permits) {
            if (mustSucceed && permits.tryAcquire()) {
                return Releasables.releaseOnce(permits::release);
            } else {
                return null;
            }
        }

        @Nullable // if not all permits were acquired
        private Releasable tryAcquireAllPermits(Semaphore permits) {
            if (mustSucceed && permits.tryAcquire(Integer.MAX_VALUE)) {
                return Releasables.releaseOnce(() -> permits.release(Integer.MAX_VALUE));
            } else {
                return null;
            }
        }

        public AtomicInteger runnables = new AtomicInteger(0);

        private AbstractRunnable execute(CheckedRunnable<Exception> runnable) {
            runnables.getAndIncrement();
            return new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logAndCheck(e);
                }

                @Override
                protected void doRun() throws Exception {
                    try {
                        runnable.run();
                    } catch (Throwable e) { // catch both Exceptions and Errors
                        logAndCheck(e);
                    }
                }

                @Override
                public void onRejection(Exception e) {
                    // ok, shutting down
                }

                @Override
                public void onAfter() {
                    runnables.getAndDecrement();
                }
            };
        }

        public AtomicInteger listeners = new AtomicInteger(0);

        private <T> ActionListener<T> execute(CheckedConsumer<T, Exception> consumer) {
            listeners.getAndIncrement();
            return new ActionListener<>() {
                @Override
                public void onResponse(T t) {
                    try {
                        consumer.accept(t);
                    } catch (Throwable e) { // catch both Exceptions and Errors
                        logAndCheck(e);
                    } finally {
                        listeners.getAndDecrement();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logAndCheck(e);
                }
            };
        }

        private void logAndCheck(Throwable e) {
            if (mustSucceed) {
                final AssertionError assertionError = new AssertionError("unexpected", e);
                TrackedCluster.logger.error("test failed", assertionError);
                throw assertionError;
            } else {
                logger.error("ignoring the following throwable", e);
            }
        }

        TrackedCluster(InternalTestCluster cluster, Set<String> masterNodeNames, Set<String> dataNodeNames, boolean mustSucceed) {
            this.mustSucceed = mustSucceed;
            this.completedSnapshotLatch = new CountDownLatch(mustSucceed ? 30 : 10);
            this.cluster = cluster;
            for (String nodeName : cluster.getNodeNames()) {
                nodes.put(nodeName, new TrackedNode(nodeName, masterNodeNames.contains(nodeName), dataNodeNames.contains(nodeName)));
            }

            final int repoCount = between(1, mustSucceed ? 3 : 2);
            for (int i = 0; i < repoCount; i++) {
                final String repositoryName = "repo-" + i;
                repositories.put(repositoryName, new TrackedRepository(repositoryName, randomRepoPath()));
            }

            final int indexCount = between(1, mustSucceed ? 10 : 5);
            for (int i = 0; i < indexCount; i++) {
                final String indexName = "index-" + i;
                indices.put(indexName, new TrackedIndex(indexName));
            }
        }

        void shuffleNodes() {
            final List<TrackedNode> newNodes = new ArrayList<>(nodes.values());
            Randomness.shuffle(newNodes);
            final String masterNodeName = Optional.ofNullable(cluster.getInstance(ClusterService.class).state().nodes().getMasterNode())
                .map(DiscoveryNode::getName)
                .orElse(null);
            newNodes.sort(Comparator.comparing(tn -> tn.nodeName.equals(masterNodeName)));
            shuffledNodes = newNodes;
        }

        public void run() throws InterruptedException {
            logger.info("--> starting cluster " + (mustSucceed ? "with permits" : "without permits"));
            shuffleNodes();

            for (TrackedIndex trackedIndex : indices.values()) {
                trackedIndex.start();
            }

            for (TrackedRepository trackedRepository : repositories.values()) {
                trackedRepository.start();
                waitUntil(() -> { return trackedRepository.exists(); });
            }

            final int nodeRestarterCount = between(1, 2);
            for (int i = 0; i < nodeRestarterCount; i++) {
                startNodeRestarter();
            }

            final int snapshotterCount = between(1, 5);
            for (int i = 0; i < snapshotterCount; i++) {
                startSnapshotter();
            }

            final int partialSnapshotterCount = between(1, 5);
            for (int i = 0; i < partialSnapshotterCount; i++) {
                startPartialSnapshotter();
            }

            final int clonerCount = between(0, 5);
            for (int i = 0; i < clonerCount; i++) {
                startCloner();
            }

            final int deleterCount = between(0, mustSucceed ? 3 : 5);
            for (int i = 0; i < deleterCount; i++) {
                startSnapshotDeleter();
            }

            final int restorerCount = between(0, mustSucceed ? 3 : 5);
            for (int i = 0; i < restorerCount; i++) {
                startRestorer();
            }

            final int cleanerCount = between(0, mustSucceed ? 2 : 5);
            for (int i = 0; i < cleanerCount; i++) {
                startCleaner();
            }

            if (completedSnapshotLatch.await(mustSucceed ? 30 : 60, TimeUnit.SECONDS)) {
                logger.info("--> did complete target snapshot count, finishing test");
            } else {
                logger.info("--> did not complete target snapshot count in desired timeframe, giving up");
            }

            assertTrue(shouldStop.compareAndSet(false, true));

            if (mustSucceed) {
                final long permitDeadlineMillis = threadPool.relativeTimeInMillis() + TimeUnit.MINUTES.toMillis(2);

                final List<String> failedPermitAcquisitions = new ArrayList<>();
                acquirePermitsAtEnd(
                    repositories.values().stream().map(n -> Tuple.tuple(n.repositoryName, n.permits)),
                    failedPermitAcquisitions,
                    permitDeadlineMillis
                );
                acquirePermitsAtEnd(
                    snapshots.values().stream().map(n -> Tuple.tuple(n.snapshotName, n.permits)),
                    failedPermitAcquisitions,
                    permitDeadlineMillis
                );
                acquirePermitsAtEnd(
                    indices.values().stream().map(n -> Tuple.tuple(n.indexName, n.permits)),
                    failedPermitAcquisitions,
                    permitDeadlineMillis
                );
                acquirePermitsAtEnd(
                    nodes.values().stream().map(n -> Tuple.tuple(n.nodeName, n.permits)),
                    failedPermitAcquisitions,
                    permitDeadlineMillis
                );

                if (failedPermitAcquisitions.isEmpty() == false) {
                    logger.warn("--> failed to acquire all permits: {}", failedPermitAcquisitions);
                    logger.info(
                        "--> current cluster state:\n{}",
                        Strings.toString(client().admin().cluster().prepareState().get().getState(), true, true)
                    );
                    fail("failed to acquire all permits: " + failedPermitAcquisitions);
                }
                logger.info("--> acquired all permits");
            }

            if (ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS)) {
                logger.info("--> threadpool termination successful");
            } else {
                logger.warn("--> threadpool termination timed out");
                logger.info(
                    "--> current cluster state:\n{}",
                    Strings.toString(client().admin().cluster().prepareState().get().getState(), true, true)
                );
            }

            logger.info("--> waiting for runnables to finish");
            waitUntil(() -> { return runnables.get() == 0; });
            logger.info("--> waiting for listeners to finish");
            waitUntil(() -> { return listeners.get() == 0; });

            logger.info("--> wiping indices");
            cluster.wipeIndices(indices.keySet().toArray(new String[indices.keySet().size()]));
            logger.info("--> wiping repositories");
            cluster.wipeRepositories(repositories.keySet().toArray(new String[repositories.keySet().size()]));
        }

        private void acquirePermitsAtEnd(
            Stream<Tuple<String, Semaphore>> labelledPermits,
            List<String> failedPermitAcquisitions,
            long permitDeadlineMillis
        ) {
            labelledPermits.forEach(labelledPermit -> {
                final long remainingMillis = Math.max(1L, permitDeadlineMillis - threadPool.relativeTimeInMillis());
                final String label = labelledPermit.v1();
                logger.info("--> acquiring permit [{}] with timeout of [{}ms]", label, remainingMillis);
                try {
                    if (labelledPermit.v2().tryAcquire(Integer.MAX_VALUE, remainingMillis, TimeUnit.MILLISECONDS)) {
                        logger.info("--> acquired permit [{}]", label);
                    } else {
                        logger.warn("--> failed to acquire permit [{}]", label);
                        logger.info(
                            "--> current cluster state:\n{}",
                            Strings.toString(client().admin().cluster().prepareState().get().getState(), true, true)
                        );
                        logger.info(
                            "--> hot threads:\n{}",
                            client().admin()
                                .cluster()
                                .prepareNodesHotThreads()
                                .setThreads(99999)
                                .setIgnoreIdleThreads(false)
                                .get()
                                .getNodes()
                                .stream()
                                .map(NodeHotThreads::getHotThreads)
                                .collect(Collectors.joining("\n"))
                        );
                        failedPermitAcquisitions.add(label);
                    }
                } catch (InterruptedException e) {
                    logger.warn("--> interrupted while acquiring permit [{}]", label);
                    Thread.currentThread().interrupt();
                    logAndCheck(e);
                }
            });
        }

        private void enqueueAction(final CheckedRunnable<Exception> action) {
            if (shouldStop.get()) {
                return;
            }

            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, execute(action));
        }

        private void startRestorer() {
            enqueueAction(() -> {
                boolean startedRestore = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (mustSucceed && localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (mustSucceed && localReleasables.add(trackedSnapshot.tryAcquirePermit()) == null) {
                        return;
                    }

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    logger.info(
                        "--> listing indices in [{}:{}] in preparation for restoring",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName
                    );

                    trackedSnapshot.getSnapshotInfo(client(), execute(snapshotInfo -> restoreSnapshot(snapshotInfo, releaseAll)));

                    startedRestore = true;
                } finally {
                    if (startedRestore == false) {
                        startRestorer();
                    }
                }
            });
        }

        private void restoreSnapshot(SnapshotInfo snapshotInfo, Releasable releasePreviousStep) {
            boolean startedRestore = false;
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                localReleasables.add(releasePreviousStep);

                if (shouldStop.get()) {
                    return;
                }

                boolean restoreSpecificIndicesTmp = randomBoolean();
                final List<String> indicesToRestoreList = new ArrayList<>(snapshotInfo.indices().size());
                final List<String> indicesToCloseList = new ArrayList<>(snapshotInfo.indices().size());
                final List<String> indicesToDeleteList = new ArrayList<>(snapshotInfo.indices().size());
                for (String indexName : snapshotInfo.indices()) {
                    if (snapshotInfo.shardFailures()
                        .stream()
                        .anyMatch(snapshotShardFailure -> snapshotShardFailure.getShardId().getIndexName().equals(indexName))) {

                        restoreSpecificIndicesTmp = true;
                        continue;
                    }
                    if (randomBoolean()) {
                        if (mustSucceed == false || localReleasables.add(tryAcquireAllPermits(indices.get(indexName).permits)) != null) {
                            indicesToRestoreList.add(indexName);

                            final int snapshotShardCount = snapshotInfo.indexSnapshotDetails().get(indexName).getShardCount();
                            final int indexShardCount = indices.get(indexName).shardCount;
                            if (snapshotShardCount == indexShardCount && randomBoolean()) {
                                indicesToCloseList.add(indexName);
                            } else {
                                indicesToDeleteList.add(indexName);
                                indices.get(indexName).shardCount = snapshotShardCount;
                            }
                        } else {
                            restoreSpecificIndicesTmp = true;
                        }
                    } else {
                        restoreSpecificIndicesTmp = true;
                    }
                }
                final boolean restoreSpecificIndices = restoreSpecificIndicesTmp;

                if (indicesToRestoreList.isEmpty()) {
                    logger.info(
                        "--> could not obtain exclusive lock on any indices in [{}:{}] for restore",
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    return;
                }

                final Releasable releaseAll = localReleasables.transfer();

                final String[] indicesToRestore = indicesToRestoreList.toArray(new String[0]);
                final String[] indicesToClose = indicesToCloseList.toArray(new String[0]);
                final String[] indicesToDelete = indicesToDeleteList.toArray(new String[0]);

                final StepListener<Void> closeIndicesStep = new StepListener<>();
                final StepListener<Void> deleteIndicesStep = new StepListener<>();

                if (indicesToClose.length > 0) {
                    logger.info(
                        "--> closing indices {} in preparation for restoring from [{}:{}]",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );
                    client().admin().indices().prepareClose(indicesToClose).execute(execute(closeIndexResponse -> {
                        logger.info(
                            "--> finished closing indices {} in preparation for restoring from [{}:{}]",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        if (mustSucceed) {
                            assertTrue(closeIndexResponse.isAcknowledged());
                            assertTrue(closeIndexResponse.isShardsAcknowledged());
                        }
                        closeIndicesStep.onResponse(null);
                    }));
                } else {
                    closeIndicesStep.onResponse(null);
                }

                if (indicesToDelete.length > 0) {
                    logger.info(
                        "--> deleting indices {} in preparation for restoring from [{}:{}]",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );
                    client().admin().indices().prepareDelete(indicesToDelete).execute(execute(deleteIndicesResponse -> {
                        logger.info(
                            "--> finished deleting indices {} in preparation for restoring from [{}:{}]",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        if (mustSucceed) assertTrue(deleteIndicesResponse.isAcknowledged());
                        deleteIndicesStep.onResponse(null);
                    }));
                } else {
                    deleteIndicesStep.onResponse(null);
                }

                closeIndicesStep.addListener(execute(ignored1 -> deleteIndicesStep.addListener(execute(ignored2 -> {

                    final RestoreSnapshotRequestBuilder restoreSnapshotRequestBuilder = client().admin()
                        .cluster()
                        .prepareRestoreSnapshot(snapshotInfo.repository(), snapshotInfo.snapshotId().getName());

                    if (restoreSpecificIndices) {
                        restoreSnapshotRequestBuilder.setIndices(indicesToRestore);
                    }

                    logger.info(
                        "--> restoring indices {}{} from [{}:{}]",
                        restoreSpecificIndices ? "" : "*=",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    restoreSnapshotRequestBuilder.execute(execute(restoreSnapshotResponse -> {
                        logger.info(
                            "--> triggered restore of indices {} from [{}:{}], waiting for green health",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );

                        prepareClusterHealthRequest(indicesToRestore).setWaitForGreenStatus()
                            .setWaitForNoInitializingShards(true)
                            .execute(execute(clusterHealthResponse -> {

                                if (clusterHealthResponse.isTimedOut() == false) {
                                    logger.info(
                                        "--> indices {} successfully restored from [{}:{}]",
                                        indicesToRestoreList,
                                        snapshotInfo.repository(),
                                        snapshotInfo.snapshotId().getName()
                                    );
                                }

                                Releasables.close(releaseAll);
                                if (mustSucceed) assertFalse(clusterHealthResponse.isTimedOut());
                                startRestorer();
                            }));
                    }));
                }))));

                startedRestore = true;
            } finally {
                if (startedRestore == false) {
                    startRestorer();
                }
            }
        }

        private void startCloner() {
            enqueueAction(() -> {
                boolean startedClone = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (mustSucceed && localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = acquireClient(localReleasables);

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (mustSucceed && localReleasables.add(trackedSnapshot.tryAcquirePermit()) == null) {
                        return;
                    }

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final StepListener<List<String>> getIndicesStep = new StepListener<>();

                    logger.info(
                        "--> listing indices in [{}:{}] in preparation for cloning",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName
                    );

                    trackedSnapshot.getSnapshotInfo(client, execute(snapshotInfo -> {
                        final Set<String> failedShardIndices = snapshotInfo.shardFailures()
                            .stream()
                            .map(ShardOperationFailedException::index)
                            .collect(Collectors.toSet());
                        final Set<String> cloneableIndices = new HashSet<>(snapshotInfo.indices());
                        cloneableIndices.removeAll(failedShardIndices);

                        if (cloneableIndices.isEmpty()) {
                            getIndicesStep.onResponse(Collections.emptyList());
                            return;
                        }

                        if (failedShardIndices.isEmpty() && randomBoolean()) {
                            getIndicesStep.onResponse(Collections.singletonList("*"));
                            return;
                        }

                        getIndicesStep.onResponse(randomSubsetOf(between(1, cloneableIndices.size()), cloneableIndices));
                    }));

                    getIndicesStep.addListener(execute(indexNames -> {

                        if (indexNames.isEmpty()) {
                            logger.info(
                                "--> no successful indices in [{}:{}], skipping clone",
                                trackedSnapshot.trackedRepository.repositoryName,
                                trackedSnapshot.snapshotName
                            );
                            Releasables.close(releaseAll);
                            startCloner();
                            return;
                        }

                        final String cloneName = "snapshot-clone-" + snapshotCounter.incrementAndGet();

                        logger.info(
                            "--> starting clone of [{}:{}] as [{}:{}] with indices {}",
                            trackedSnapshot.trackedRepository.repositoryName,
                            trackedSnapshot.snapshotName,
                            trackedSnapshot.trackedRepository.repositoryName,
                            cloneName,
                            indexNames
                        );

                        client.admin()
                            .cluster()
                            .prepareCloneSnapshot(trackedSnapshot.trackedRepository.repositoryName, trackedSnapshot.snapshotName, cloneName)
                            .setIndices(indexNames.toArray(new String[0]))
                            .execute(execute(acknowledgedResponse -> {
                                Releasables.close(releaseAll);
                                if (mustSucceed) assertTrue(acknowledgedResponse.isAcknowledged());
                                if (acknowledgedResponse.isAcknowledged()) {
                                    completedSnapshotLatch.countDown();
                                    logger.info(
                                        "--> completed snapshot clone of [{}:{}] as [{}:{}]",
                                        trackedSnapshot.trackedRepository.repositoryName,
                                        trackedSnapshot.snapshotName,
                                        trackedSnapshot.trackedRepository.repositoryName,
                                        cloneName
                                    );
                                }
                                startCloner();
                            }));
                    }));

                    startedClone = true;
                } finally {
                    if (startedClone == false) {
                        startCloner();
                    }
                }
            });
        }

        private void startSnapshotDeleter() {
            enqueueAction(() -> {

                boolean startedDeletion = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    if (mustSucceed && localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = acquireClient(localReleasables);

                    final List<String> snapshotNames = new ArrayList<>();
                    final TrackedRepository targetRepository = getOneRepositoryWithSnapshots(localReleasables, snapshotNames);
                    if (targetRepository == null) return;

                    logger.info("--> starting deletion of [{}:{}]", targetRepository.repositoryName, snapshotNames);

                    final Releasable releaseAll = localReleasables.transfer();

                    client.admin()
                        .cluster()
                        .prepareDeleteSnapshot(targetRepository.repositoryName, snapshotNames.toArray(new String[0]))
                        .execute(execute(acknowledgedResponse -> {
                            if (mustSucceed) {
                                assertTrue(acknowledgedResponse.isAcknowledged());
                                for (String snapshotName : snapshotNames) {
                                    assertThat(snapshots.remove(snapshotName), notNullValue());
                                }
                            }
                            Releasables.close(releaseAll); // must only release snapshot after removing it from snapshots map
                            if (acknowledgedResponse.isAcknowledged()) {
                                logger.info("--> completed deletion of [{}:{}]", targetRepository.repositoryName, snapshotNames);
                            }
                            startSnapshotDeleter();
                        }));

                    startedDeletion = true;

                } finally {
                    if (startedDeletion == false) {
                        startSnapshotDeleter();
                    }
                }
            });
        }

        @Nullable // if no blocks could be acquired
        private TrackedRepository getOneRepositoryWithSnapshots(TransferableReleasables localReleasables, List<String> snapshotNames) {
            final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
            TrackedRepository targetRepository = null;
            Randomness.shuffle(trackedSnapshots);
            for (TrackedSnapshot trackedSnapshot : trackedSnapshots) {
                if ((targetRepository == null || trackedSnapshot.trackedRepository == targetRepository)
                    && (snapshotNames.isEmpty() || randomBoolean())
                    && snapshots.get(trackedSnapshot.snapshotName) == trackedSnapshot) {
                    if (mustSucceed == false || localReleasables.add(trackedSnapshot.tryAcquireAllPermits()) != null) {
                        targetRepository = trackedSnapshot.trackedRepository;
                        snapshotNames.add(trackedSnapshot.snapshotName);
                    }
                }
            }

            if (targetRepository != null) {
                assertFalse(targetRepository.repositoryName, snapshotNames.isEmpty());
            }
            return targetRepository;
        }

        private void startCleaner() {
            enqueueAction(() -> {

                boolean startedCleanup = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    if (mustSucceed && localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = acquireClient(localReleasables);

                    for (TrackedRepository trackedRepository : repositories.values()) {
                        // cleanup forbids all concurrent snapshot activity
                        if (mustSucceed && localReleasables.add(tryAcquireAllPermits(trackedRepository.permits)) == null) {
                            return;
                        }
                    }

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());

                    final Releasable releaseAll = localReleasables.transfer();

                    logger.info("--> starting cleanup of [{}]", trackedRepository.repositoryName);
                    client.admin()
                        .cluster()
                        .prepareCleanupRepository(trackedRepository.repositoryName)
                        .execute(execute(cleanupRepositoryResponse -> {
                            final RepositoryCleanupResult result = cleanupRepositoryResponse.result();
                            if (result.bytes() > 0L || result.blobs() > 0L) {
                                // we could legitimately run into dangling blobs as the result of a shard snapshot failing half-way
                                // through the snapshot because of a concurrent index-close or -delete. The second round of cleanup on
                                // the same repository however must always fully remove any dangling blobs since we block all concurrent
                                // operations on the repository here
                                client.admin()
                                    .cluster()
                                    .prepareCleanupRepository(trackedRepository.repositoryName)
                                    .execute(execute(secondCleanupRepositoryResponse -> {
                                        final RepositoryCleanupResult secondCleanupResult = secondCleanupRepositoryResponse.result();
                                        if (mustSucceed) {
                                            assertThat(Strings.toString(secondCleanupResult), secondCleanupResult.blobs(), equalTo(0L));
                                            assertThat(Strings.toString(secondCleanupResult), secondCleanupResult.bytes(), equalTo(0L));
                                        }
                                        Releasables.close(releaseAll);
                                        logger.info("--> completed second cleanup of [{}]", trackedRepository.repositoryName);
                                        startCleaner();
                                    }));
                            } else {
                                Releasables.close(releaseAll);
                                logger.info("--> completed cleanup of [{}]", trackedRepository.repositoryName);
                                startCleaner();
                            }
                        }));

                    startedCleanup = true;
                } finally {
                    if (startedCleanup == false) {
                        startCleaner();
                    }
                }
            });
        }

        private void startSnapshotter() {
            enqueueAction(() -> {

                boolean startedSnapshot = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    // separate TransferableReleasables for blocking node restarts & index deletion so we can release these blocks and
                    // permit data node restarts and index deletions as soon as the snapshot starts
                    final TransferableReleasables releasableAfterStart = new TransferableReleasables();
                    localReleasables.add(releasableAfterStart);

                    if (mustSucceed && releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    if (mustSucceed) assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = acquireClient(localReleasables);

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (mustSucceed && localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually()) {
                            if (mustSucceed == false || localReleasables.add(tryAcquirePermit(trackedIndex.permits)) != null) {
                                targetIndexNames.add(trackedIndex.indexName);
                            } else {
                                snapshotSpecificIndicesTmp = true;
                            }
                        } else {
                            snapshotSpecificIndicesTmp = true;
                        }
                    }
                    final boolean snapshotSpecificIndices = snapshotSpecificIndicesTmp;

                    if (snapshotSpecificIndices && targetIndexNames.isEmpty()) {
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                    final String snapshotName = "snapshot-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> waiting for yellow health of [{}] before creating snapshot [{}:{}]",
                        targetIndexNames,
                        trackedRepository.repositoryName,
                        snapshotName
                    );

                    prepareClusterHealthRequest(targetIndexNames.toArray(String[]::new)).setWaitForYellowStatus().execute(ensureYellowStep);

                    ensureYellowStep.addListener(execute(clusterHealthResponse -> {
                        if (mustSucceed) {
                            assertFalse("timed out waiting for yellow state of " + targetIndexNames, clusterHealthResponse.isTimedOut());
                        }

                        logger.info(
                            "--> take snapshot [{}:{}] with indices [{}{}]",
                            trackedRepository.repositoryName,
                            snapshotName,
                            snapshotSpecificIndices ? "" : "*=",
                            targetIndexNames
                        );

                        final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = client().admin()
                            .cluster()
                            .prepareCreateSnapshot(trackedRepository.repositoryName, snapshotName);

                        if (snapshotSpecificIndices) {
                            createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                        }

                        if (randomBoolean()) {
                            createSnapshotRequestBuilder.setWaitForCompletion(true);
                            createSnapshotRequestBuilder.execute(execute(createSnapshotResponse -> {
                                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                                if (snapshotInfo.state().equals(SnapshotState.SUCCESS)) {
                                    logger.info("--> completed snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                    Releasables.close(releaseAll);
                                    completedSnapshotLatch.countDown();
                                } else {
                                    if (mustSucceed) fail(stringFromSnapshotInfo(snapshotInfo));
                                }
                                startSnapshotter();
                            }));
                        } else {
                            createSnapshotRequestBuilder.execute(execute(createSnapshotResponse -> {
                                logger.info("--> started snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                Releasables.close(releasableAfterStart.transfer());
                                pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                                    snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                                    logger.info("--> completed snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                    completedSnapshotLatch.countDown();
                                    startSnapshotter();
                                });
                            }));
                        }

                    }));

                    startedSnapshot = true;
                } finally {
                    if (startedSnapshot == false) {
                        startSnapshotter();
                    }
                }
            });
        }

        private void startPartialSnapshotter() {
            enqueueAction(() -> {

                boolean startedSnapshot = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    // separate TransferableReleasables for blocking node restarts & index deletion so we can release these blocks and
                    // permit data node restarts and index deletions as soon as the snapshot starts
                    final TransferableReleasables releasableAfterStart = new TransferableReleasables();
                    localReleasables.add(releasableAfterStart);

                    if (mustSucceed && releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    if (mustSucceed) assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = acquireClient(localReleasables);

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (mustSucceed && localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually()) {
                            if (mustSucceed == false || releasableAfterStart.add(tryAcquirePermit(trackedIndex.permits)) != null) {
                                targetIndexNames.add(trackedIndex.indexName);
                            } else {
                                snapshotSpecificIndicesTmp = true;
                            }
                        } else {
                            snapshotSpecificIndicesTmp = true;
                        }
                    }
                    final boolean snapshotSpecificIndices = snapshotSpecificIndicesTmp;

                    if (snapshotSpecificIndices && targetIndexNames.isEmpty()) {
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final String snapshotName = "snapshot-partial-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> take partial snapshot [{}:{}] with indices [{}{}]",
                        trackedRepository.repositoryName,
                        snapshotName,
                        snapshotSpecificIndices ? "" : "*=",
                        targetIndexNames
                    );

                    final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = client().admin()
                        .cluster()
                        .prepareCreateSnapshot(trackedRepository.repositoryName, snapshotName)
                        .setPartial(true);

                    if (snapshotSpecificIndices) {
                        createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                    }

                    final boolean abortSnapshot = randomBoolean();
                    final Runnable abortRunnable;
                    if (abortSnapshot) {
                        try (TransferableReleasables abortReleasables = new TransferableReleasables()) {

                            if (mustSucceed) assertNotNull(abortReleasables.add(blockFullClusterRestart()));
                            final Client abortClient = acquireClient(abortReleasables);

                            if (mustSucceed) assertNotNull(abortReleasables.add(tryAcquirePermit(trackedRepository.permits)));

                            final DeleteSnapshotRequestBuilder deleteSnapshotRequestBuilder = abortClient.admin()
                                .cluster()
                                .prepareDeleteSnapshot(trackedRepository.repositoryName, snapshotName);

                            final Releasable abortReleasable = abortReleasables.transfer();

                            abortRunnable = execute(() -> {
                                logger.info("--> aborting/deleting snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                deleteSnapshotRequestBuilder.execute(new ActionListener<>() {
                                    @Override
                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                        logger.info("--> aborted/deleted snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                        Releasables.close(abortReleasable);
                                        if (mustSucceed) assertTrue(acknowledgedResponse.isAcknowledged());
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        Releasables.close(abortReleasable);
                                        if (ExceptionsHelper.unwrapCause(e) instanceof SnapshotMissingException) {
                                            // processed before the snapshot even started
                                            logger.info(
                                                "--> abort/delete of [{}:{}] got snapshot missing",
                                                trackedRepository.repositoryName,
                                                snapshotName
                                            );
                                        } else {
                                            logAndCheck(e);
                                        }
                                    }
                                });
                            });
                        }
                    } else {
                        abortRunnable = () -> {};
                    }

                    createSnapshotRequestBuilder.execute(execute(createSnapshotResponse -> {
                        logger.info("--> started partial snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                        Releasables.close(releasableAfterStart.transfer());
                        pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                            if (abortSnapshot == false) {
                                logger.info("--> completed snapshot partial [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                                completedSnapshotLatch.countDown();
                            }
                            startPartialSnapshotter();
                        });
                    }));

                    abortRunnable.run();

                    startedSnapshot = true;
                } finally {
                    if (startedSnapshot == false) {
                        startPartialSnapshotter();
                    }
                }
            });
        }

        private void pollForSnapshotCompletion(
            Client client,
            String repositoryName,
            String snapshotName,
            Releasable onCompletion,
            Runnable onSuccess
        ) {
            threadPool.executor(CLIENT)
                .execute(
                    execute(
                        () -> client.admin()
                            .cluster()
                            .prepareGetSnapshots(repositoryName)
                            .setCurrentSnapshot()
                            .execute(execute(getSnapshotsResponse -> {
                                if (getSnapshotsResponse.getSnapshots()
                                    .stream()
                                    .noneMatch(snapshotInfo -> snapshotInfo.snapshotId().getName().equals(snapshotName))) {

                                    logger.info("--> snapshot [{}:{}] no longer running", repositoryName, snapshotName);
                                    Releasables.close(onCompletion);
                                    onSuccess.run();
                                } else {
                                    pollForSnapshotCompletion(client, repositoryName, snapshotName, onCompletion, onSuccess);
                                }
                            }))
                    )
                );
        }

        private void startNodeRestarter() {
            enqueueAction(() -> {
                boolean restarting = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    if (usually()) {
                        return;
                    }

                    final ArrayList<TrackedNode> trackedNodes = new ArrayList<>(shuffledNodes);
                    Collections.reverse(trackedNodes);

                    for (TrackedNode trackedNode : trackedNodes) {
                        if (mustSucceed == false || localReleasables.add(tryAcquireAllPermits(trackedNode.permits)) != null) {

                            final String nodeName = trackedNode.nodeName;
                            final Releasable releaseAll = localReleasables.transfer();

                            threadPool.generic().execute(execute(() -> {
                                logger.info("--> restarting [{}]", nodeName);
                                cluster.restartNode(nodeName);
                                logger.info("--> finished restarting [{}]", nodeName);
                                shuffleNodes();
                                Releasables.close(releaseAll);
                                startNodeRestarter();
                            }));

                            restarting = true;
                            return;
                        }
                    }

                } finally {
                    if (restarting == false) {
                        startNodeRestarter();
                    }
                }
            });
        }

        @Nullable // if we couldn't block node restarts
        private Releasable blockNodeRestarts() {
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                for (TrackedNode trackedNode : nodes.values()) {
                    if (localReleasables.add(tryAcquirePermit(trackedNode.getPermits())) == null) {
                        return null;
                    }
                }
                return localReleasables.transfer();
            }
        }

        /**
         * Try and block the restart of a majority of the master nodes, which therefore prevents a full-cluster restart from occurring.
         */
        @Nullable // if we couldn't block enough master node restarts
        private Releasable blockFullClusterRestart() {
            // Today we block all master failovers to avoid things like TransportMasterNodeAction-led retries which might fail e.g. because
            // the snapshot already exists).

            // TODO generalise this so that it succeeds as soon as it's acquired a permit on >1/2 of the master-eligible nodes
            final List<TrackedNode> masterNodes = shuffledNodes.stream().filter(TrackedNode::isMasterNode).toList();
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                for (TrackedNode trackedNode : masterNodes) {
                    if (localReleasables.add(tryAcquirePermit(trackedNode.getPermits())) == null) {
                        return null;
                    }
                }
                return localReleasables.transfer();
            }
        }

        private static <E> E getRandomFromCollection(Collection<E> e) {
            return e.stream().skip(randomIntBetween(0, e.size() - 1)).findFirst().get();
        }

        /**
         * Acquire a client (i.e. block the client node from restarting) in a situation where we know that such a block can be obtained,
         * since previous acquisitions mean that at least one node is already blocked from restarting.
         */
        private ReleasableClient acquireReleasableClient() {
            for (TrackedNode trackedNode : shuffledNodes) {
                final Releasable permit = tryAcquirePermit(trackedNode.getPermits());
                if (permit != null) {
                    return new ReleasableClient(permit, client(trackedNode.nodeName));
                }
            }

            final AssertionError assertionError = new AssertionError("could not acquire client");
            logger.error("acquireClient", assertionError);
            throw assertionError;
        }

        private Client acquireClient(TransferableReleasables releasables) {
            if (mustSucceed) {
                return releasables.add(acquireReleasableClient()).getClient();
            } else {
                return client(getRandomFromCollection(nodes.values()).nodeName);
            }
        }

        /**
         * Tracks a repository in the cluster, and occasionally removes it and adds it back if no other activity holds any of its permits.
         */
        private class TrackedRepository {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String repositoryName;
            private final Path location;
            private boolean exists = false;

            private TrackedRepository(String repositoryName, Path location) {
                this.repositoryName = repositoryName;
                this.location = location;
            }

            public boolean exists() {
                return exists;
            }

            @Override
            public String toString() {
                return "TrackedRepository[" + repositoryName + "]";
            }

            public void start() {
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    if (mustSucceed) {
                        assertNotNull(localReleasables.add(blockNodeRestarts()));
                        assertNotNull(localReleasables.add(tryAcquireAllPermits(permits)));
                    }
                    final Client client = acquireClient(localReleasables);
                    putRepositoryAndContinue(client, localReleasables.transfer());
                }
            }

            private void putRepositoryAndContinue(Client client, Releasable releasable) {
                logger.info("--> put repo [{}]", repositoryName);
                client.admin()
                    .cluster()
                    .preparePutRepository(repositoryName)
                    .setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), location))
                    .execute(execute(acknowledgedResponse -> {
                        if (mustSucceed) assertTrue(acknowledgedResponse.isAcknowledged());
                        if (acknowledgedResponse.isAcknowledged()) {
                            exists = true;
                            logger.info("--> finished put repo [{}]", repositoryName);
                        }
                        Releasables.close(releasable);
                        scheduleRemoveAndAdd();
                    }));
            }

            private void scheduleRemoveAndAdd() {
                enqueueAction(() -> {

                    boolean replacingRepo = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (usually()) {
                            return;
                        }

                        if (mustSucceed && localReleasables.add(tryAcquireAllPermits(permits)) == null) {
                            return;
                        }

                        if (mustSucceed && localReleasables.add(blockFullClusterRestart()) == null) {
                            return;
                        }

                        final Client client = acquireClient(localReleasables);

                        final Releasable releaseAll = localReleasables.transfer();

                        logger.info("--> delete repo [{}]", repositoryName);
                        client().admin().cluster().prepareDeleteRepository(repositoryName).execute(execute(acknowledgedResponse -> {
                            if (mustSucceed) assertTrue(acknowledgedResponse.isAcknowledged());
                            if (acknowledgedResponse.isAcknowledged()) {
                                exists = false;
                                logger.info("--> finished delete repo [{}]", repositoryName);
                            }
                            putRepositoryAndContinue(client, releaseAll);
                        }));

                        replacingRepo = true;
                    } finally {
                        if (replacingRepo == false) {
                            scheduleRemoveAndAdd();
                        }
                    }
                });
            }

        }

        private class TrackedIndex {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String indexName;

            // these fields are only changed when all permits held by the delete/recreate process:
            private int shardCount;
            private Semaphore docPermits;

            private TrackedIndex(String indexName) {
                this.indexName = indexName;
            }

            @Override
            public String toString() {
                return "TrackedIndex[" + indexName + "]";
            }

            public void start() {
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    if (mustSucceed) {
                        assertNotNull(localReleasables.add(blockNodeRestarts()));
                        assertNotNull(localReleasables.add(tryAcquireAllPermits(permits)));
                    }
                    createIndexAndContinue(localReleasables.transfer());
                }
            }

            private void createIndexAndContinue(Releasable releasable) {
                shardCount = between(1, 5);
                docPermits = new Semaphore(mustSucceed ? between(1000, 3000) : between(10, 200));
                logger.info("--> create index [{}] with max [{}] docs", indexName, docPermits.availablePermits());
                client().admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shardCount)
                            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), between(0, cluster.numDataNodes() - 1))
                    )
                    .execute(execute(response -> {
                        if (mustSucceed) assertTrue(response.isAcknowledged());
                        Releasables.close(releasable);
                        if (response.isAcknowledged()) {
                            logger.info("--> finished create index [{}]", indexName);
                        }
                        scheduleIndexingAndPossibleDelete();
                    }));
            }

            private void scheduleIndexingAndPossibleDelete() {
                enqueueAction(() -> {

                    boolean forked = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (mustSucceed && localReleasables.add(blockNodeRestarts()) == null) {
                            return;
                        }

                        if (usually()) {
                            // index some more docs

                            if (mustSucceed && localReleasables.add(tryAcquirePermit(permits)) == null) {
                                return;
                            }

                            final int maxDocCount = mustSucceed ? docPermits.drainPermits() : docPermits.availablePermits();
                            assert maxDocCount >= 0 : maxDocCount;
                            if (maxDocCount == 0) {
                                return;
                            }
                            final int docCount = between(1, Math.min(maxDocCount, 200));
                            if (mustSucceed) docPermits.release(maxDocCount - docCount);

                            final Releasable releaseAll = localReleasables.transfer();

                            final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                            logger.info("--> waiting for yellow health of [{}] prior to indexing [{}] docs", indexName, docCount);

                            prepareClusterHealthRequest(indexName).setWaitForYellowStatus().execute(ensureYellowStep);

                            final StepListener<BulkResponse> bulkStep = new StepListener<>();

                            ensureYellowStep.addListener(execute(clusterHealthResponse -> {

                                if (mustSucceed) assertFalse(
                                    "timed out waiting for yellow state of [" + indexName + "]",
                                    clusterHealthResponse.isTimedOut()
                                );

                                final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk(indexName);

                                logger.info("--> indexing [{}] docs into [{}]", docCount, indexName);

                                for (int i = 0; i < docCount; i++) {
                                    bulkRequestBuilder.add(
                                        new IndexRequest().source(
                                            jsonBuilder().startObject().field("field-" + between(1, 5), randomAlphaOfLength(10)).endObject()
                                        )
                                    );
                                }

                                bulkRequestBuilder.execute(bulkStep);
                            }));

                            bulkStep.addListener(execute(bulkItemResponses -> {
                                for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                                    if (mustSucceed) assertNull(bulkItemResponse.getFailure());
                                }

                                logger.info("--> indexing into [{}] finished", indexName);

                                Releasables.close(releaseAll);
                                scheduleIndexingAndPossibleDelete();

                            }));

                            forked = true;

                        } else if (mustSucceed == false || localReleasables.add(tryAcquireAllPermits(permits)) != null) {
                            // delete the index and create a new one

                            final Releasable releaseAll = localReleasables.transfer();

                            logger.info("--> deleting index [{}]", indexName);

                            client().admin().indices().prepareDelete(indexName).execute(execute(acknowledgedResponse -> {
                                if (mustSucceed) assertTrue(acknowledgedResponse.isAcknowledged());
                                if (acknowledgedResponse.isAcknowledged()) {
                                    logger.info("--> deleting index [{}] finished", indexName);
                                }
                                createIndexAndContinue(releaseAll);
                            }));

                            forked = true;
                        }
                    } finally {
                        if (forked == false) {
                            scheduleIndexingAndPossibleDelete();
                        }
                    }
                });
            }

        }

        // Prepares a health request with twice the default (30s) timeout that waits for all cluster tasks to finish as well as all cluster
        // nodes before returning
        private ClusterHealthRequestBuilder prepareClusterHealthRequest(String... targetIndexNames) {
            return client().admin()
                .cluster()
                .prepareHealth(targetIndexNames)
                .setTimeout(mustSucceed ? TimeValue.timeValueSeconds(60) : TimeValue.timeValueMillis(1))
                .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                .setWaitForEvents(Priority.LANGUID);
        }

        private static String stringFromSnapshotInfo(SnapshotInfo snapshotInfo) {
            return Strings.toString((b, p) -> snapshotInfo.toXContent(b, SNAPSHOT_ONLY_FORMAT_PARAMS), true, false);
        }

        /**
         * A client to a node that is blocked from restarting; close this {@link Releasable} to release the block.
         */
        private static class ReleasableClient implements Releasable {
            private final Releasable releasable;
            private final Client client;

            ReleasableClient(Releasable releasable, Client client) {
                this.releasable = releasable;
                this.client = client;
            }

            @Override
            public void close() {
                if (releasable != null) releasable.close();
            }

            Client getClient() {
                return client;
            }
        }

        /**
         * Tracks a snapshot taken by the cluster.
         */
        private class TrackedSnapshot {

            private final TrackedRepository trackedRepository;
            private final String snapshotName;
            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final AtomicReference<ListenableActionFuture<SnapshotInfo>> snapshotInfoFutureRef = new AtomicReference<>();

            TrackedSnapshot(TrackedRepository trackedRepository, String snapshotName) {
                this.trackedRepository = trackedRepository;
                this.snapshotName = snapshotName;
            }

            /*
             * Try and acquire a permit on this snapshot and the underlying repository
             */
            Releasable tryAcquirePermit() {
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    if (localReleasables.add(TrackedCluster.this.tryAcquirePermit(trackedRepository.permits)) == null) {
                        return null;
                    }

                    if (localReleasables.add(TrackedCluster.this.tryAcquirePermit(permits)) == null) {
                        return null;
                    }

                    return localReleasables.transfer();
                }
            }

            Releasable tryAcquireAllPermits() {
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    if (localReleasables.add(TrackedCluster.this.tryAcquirePermit(trackedRepository.permits)) == null) {
                        return null;
                    }

                    if (localReleasables.add(TrackedCluster.this.tryAcquireAllPermits(permits)) == null) {
                        return null;
                    }

                    return localReleasables.transfer();
                }
            }

            void getSnapshotInfo(Client client, ActionListener<SnapshotInfo> listener) {
                final ListenableActionFuture<SnapshotInfo> newFuture = new ListenableActionFuture<>();

                final boolean firstRunner = snapshotInfoFutureRef.compareAndSet(null, newFuture);

                if (firstRunner == false) {
                    if (usually()) {
                        snapshotInfoFutureRef.get().addListener(listener);
                        return;
                    }
                    // else (rarely) get it again, expecting it to be the same

                    snapshotInfoFutureRef.get()
                        .addListener(
                            execute(
                                snapshotInfo1 -> newFuture.addListener(
                                    execute(snapshotInfo2 -> assertThat(snapshotInfo1, equalTo(snapshotInfo2)))
                                )
                            )
                        );
                }

                newFuture.addListener(listener);

                TrackedCluster.logger.info(
                    "--> getting snapshot info{} for [{}:{}]",
                    firstRunner ? "" : " again",
                    trackedRepository.repositoryName,
                    snapshotName
                );
                client.admin()
                    .cluster()
                    .prepareGetSnapshots(trackedRepository.repositoryName)
                    .setSnapshots(snapshotName)
                    .execute(execute(getSnapshotsResponse -> {
                        if (mustSucceed) assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
                        final SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);
                        if (mustSucceed) assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName));
                        TrackedCluster.logger.info(
                            "--> got snapshot info for [{}:{}]{}",
                            trackedRepository.repositoryName,
                            snapshotName,
                            firstRunner ? ":\n" + stringFromSnapshotInfo(snapshotInfo) : " again"
                        );
                        newFuture.onResponse(snapshotInfo);
                    }));
            }
        }

        /**
         * Tracks a node in the cluster.
         */
        private class TrackedNode {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String nodeName;
            private final boolean isMasterNode;
            private final boolean isDataNode;

            TrackedNode(String nodeName, boolean isMasterNode, boolean isDataNode) {
                this.nodeName = nodeName;
                this.isMasterNode = isMasterNode;
                this.isDataNode = isDataNode;
            }

            Semaphore getPermits() {
                return permits;
            }

            boolean isMasterNode() {
                return isMasterNode;
            }

            @Override
            public String toString() {
                return "TrackedNode{" + nodeName + "}{" + (isMasterNode ? "m" : "") + (isDataNode ? "d" : "") + "}";
            }
        }

    }

}
