/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class SnapshotStressTestsIT extends AbstractSnapshotIntegTestCase {

    public void testRandomActivities() throws InterruptedException {
        final DiscoveryNodes discoveryNodes = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes();
        new TrackedCluster(internalCluster(), nodeNames(discoveryNodes.getMasterNodes()), nodeNames(discoveryNodes.getDataNodes())).run();
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    private static Set<String> nodeNames(Map<String, DiscoveryNode> nodesMap) {
        return nodesMap.values().stream().map(DiscoveryNode::getName).collect(Collectors.toSet());
    }

    /**
     * Encapsulates a common pattern of trying to acquire a bunch of resources and then transferring ownership elsewhere on success,
     * but releasing them on failure.
     */
    private static class TransferableReleasables implements Releasable {

        private boolean transferred = false;
        private final List<Releasable> releasables = new ArrayList<>();

        <T extends Releasable> T add(T releasable) {
            assert transferred == false : "already transferred";
            releasables.add(releasable);
            return releasable;
        }

        Releasable transfer() {
            assert transferred == false : "already transferred";
            transferred = true;
            Collections.reverse(releasables);
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
    private static Releasable tryAcquirePermit(Semaphore permits) {
        if (permits.tryAcquire()) {
            return Releasables.releaseOnce(permits::release);
        } else {
            return null;
        }
    }

    @Nullable // if not all permits were acquired
    private static Releasable tryAcquireAllPermits(Semaphore permits) {
        if (permits.tryAcquire(Integer.MAX_VALUE)) {
            return Releasables.releaseOnce(() -> permits.release(Integer.MAX_VALUE));
        } else {
            return null;
        }
    }

    private static AbstractRunnable mustSucceed(CheckedRunnable<Exception> runnable) {
        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logAndFailTest(e);
            }

            @Override
            protected void doRun() throws Exception {
                runnable.run();
            }

            @Override
            public void onRejection(Exception e) {
                // ok, shutting down
            }
        };
    }

    private static <T> ActionListener<T> mustSucceed(CheckedConsumer<T, Exception> consumer) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T t) {
                try {
                    consumer.accept(t);
                } catch (Exception e) {
                    logAndFailTest(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logAndFailTest(e);
            }
        };
    }

    private static void logAndFailTest(Exception e) {
        final AssertionError assertionError = new AssertionError("unexpected", e);
        TrackedCluster.logger.error("test failed", assertionError);
        throw assertionError;
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
     */
    private static class TrackedCluster {

        static final Logger logger = LogManager.getLogger(TrackedCluster.class);
        static final String CLIENT = "client";
        static final String NODE_RESTARTER = "node_restarter";

        private final ThreadPool threadPool = new TestThreadPool(
            "TrackedCluster",
            // a single thread for "client" activities, to limit the number of activities all starting at once
            new ScalingExecutorBuilder(CLIENT, 1, 1, TimeValue.ZERO, true, CLIENT),
            new ScalingExecutorBuilder(NODE_RESTARTER, 1, 5, TimeValue.ZERO, true, NODE_RESTARTER)
        );
        private final Executor clientExecutor = threadPool.executor(CLIENT);

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
        private final CountDownLatch completedSnapshotLatch = new CountDownLatch(30);

        TrackedCluster(InternalTestCluster cluster, Set<String> masterNodeNames, Set<String> dataNodeNames) {
            this.cluster = cluster;
            for (String nodeName : cluster.getNodeNames()) {
                nodes.put(nodeName, new TrackedNode(nodeName, masterNodeNames.contains(nodeName), dataNodeNames.contains(nodeName)));
            }

            final int repoCount = between(1, 3);
            for (int i = 0; i < repoCount; i++) {
                final String repositoryName = "repo-" + i;
                repositories.put(repositoryName, new TrackedRepository(repositoryName, randomRepoPath()));
            }

            final int indexCount = between(1, 10);
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
            shuffleNodes();

            for (TrackedIndex trackedIndex : indices.values()) {
                trackedIndex.start();
            }

            for (TrackedRepository trackedRepository : repositories.values()) {
                trackedRepository.start();
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

            final int deleterCount = between(0, 3);
            for (int i = 0; i < deleterCount; i++) {
                startSnapshotDeleter();
            }

            final int restorerCount = between(0, 3);
            for (int i = 0; i < restorerCount; i++) {
                startRestorer();
            }

            final int cleanerCount = between(0, 2);
            for (int i = 0; i < cleanerCount; i++) {
                startCleaner();
            }

            if (randomBoolean()) {
                startNodeShutdownMarker();
            }

            if (completedSnapshotLatch.await(30, TimeUnit.SECONDS)) {
                logger.info("--> completed target snapshot count, finishing test");
            } else {
                logger.info("--> did not complete target snapshot count in 30s, giving up");
            }

            assertTrue(shouldStop.compareAndSet(false, true));
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
                    Strings.toString(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState(), true, true)
                );
                fail("failed to acquire all permits: " + failedPermitAcquisitions);
            }
            logger.info("--> acquired all permits");

            if (ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS) == false) {
                logger.warn("--> threadpool termination timed out");
                logger.info(
                    "--> current cluster state:\n{}",
                    Strings.toString(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState(), true, true)
                );
            }
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
                            Strings.toString(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState(), true, true)
                        );
                        HotThreads.logLocalHotThreads(
                            logger,
                            Level.INFO,
                            "hot threads while failing to acquire permit [" + label + "]",
                            ReferenceDocs.LOGGING
                        );
                        failedPermitAcquisitions.add(label);
                    }
                } catch (InterruptedException e) {
                    logger.warn("--> interrupted while acquiring permit [{}]", label);
                    Thread.currentThread().interrupt();
                    logAndFailTest(e);
                }
            });
        }

        private void enqueueAction(final CheckedRunnable<Exception> action) {
            if (shouldStop.get()) {
                return;
            }

            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), clientExecutor, mustSucceed(action));
        }

        private void startRestorer() {
            enqueueAction(() -> {
                boolean startedRestore = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (localReleasables.add(trackedSnapshot.tryAcquirePermit()) == null) {
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

                    trackedSnapshot.getSnapshotInfo(client(), mustSucceed(snapshotInfo -> restoreSnapshot(snapshotInfo, releaseAll)));

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
                    final var trackedIndex = indices.get(indexName);
                    if (randomBoolean() && localReleasables.add(tryAcquireAllPermits(trackedIndex.permits)) != null) {

                        indicesToRestoreList.add(indexName);

                        final int snapshotShardCount = snapshotInfo.indexSnapshotDetails().get(indexName).getShardCount();
                        final int indexShardCount = trackedIndex.shardCount;
                        if (snapshotShardCount == indexShardCount
                            && randomBoolean()
                            && localReleasables.add(trackedIndex.tryAcquireClosingPermit()) != null) {
                            indicesToCloseList.add(indexName);
                        } else {
                            indicesToDeleteList.add(indexName);
                            trackedIndex.shardCount = snapshotShardCount;
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
                final String indicesToRestoreDescription = (restoreSpecificIndices ? "" : "*=") + Arrays.toString(indicesToRestore);

                if (restoreSpecificIndices == false) {
                    assertEquals(Set.copyOf(snapshotInfo.indices()), Set.of(indicesToRestore));
                }

                final ListenableFuture<Void> closeIndicesStep = new ListenableFuture<>();
                final ListenableFuture<Void> deleteIndicesStep = new ListenableFuture<>();

                if (indicesToClose.length > 0) {
                    logger.info(
                        "--> waiting for yellow health of [{}] before closing",
                        Strings.arrayToCommaDelimitedString(indicesToClose)
                    );

                    SubscribableListener.<ClusterHealthResponse>newForked(
                        l -> prepareClusterHealthRequest(indicesToClose).setWaitForYellowStatus().execute(l)
                    ).addListener(mustSucceed(clusterHealthResponse -> {
                        assertFalse(
                            "timed out waiting for yellow state of " + Strings.arrayToCommaDelimitedString(indicesToClose),
                            clusterHealthResponse.isTimedOut()
                        );

                        logger.info(
                            "--> closing indices {} in preparation for restoring {} from [{}:{}]",
                            indicesToClose,
                            indicesToRestoreDescription,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        indicesAdmin().prepareClose(indicesToClose).execute(mustSucceed(closeIndexResponse -> {
                            logger.info(
                                "--> finished closing indices {} in preparation for restoring {} from [{}:{}]",
                                indicesToClose,
                                indicesToRestoreDescription,
                                snapshotInfo.repository(),
                                snapshotInfo.snapshotId().getName()
                            );
                            assertTrue(closeIndexResponse.isAcknowledged());
                            assertTrue(closeIndexResponse.isShardsAcknowledged());
                            closeIndicesStep.onResponse(null);
                        }));
                    }));
                } else {
                    closeIndicesStep.onResponse(null);
                }

                if (indicesToDelete.length > 0) {
                    logger.info(
                        "--> deleting indices {} in preparation for restoring {} from [{}:{}]",
                        indicesToDelete,
                        indicesToRestore,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );
                    indicesAdmin().prepareDelete(indicesToDelete).execute(mustSucceed(deleteIndicesResponse -> {
                        logger.info(
                            "--> finished deleting indices {} in preparation for restoring {} from [{}:{}]",
                            indicesToDelete,
                            indicesToRestoreDescription,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        assertTrue(deleteIndicesResponse.isAcknowledged());
                        deleteIndicesStep.onResponse(null);
                    }));
                } else {
                    deleteIndicesStep.onResponse(null);
                }

                closeIndicesStep.addListener(mustSucceed(ignored1 -> deleteIndicesStep.addListener(mustSucceed(ignored2 -> {

                    final RestoreSnapshotRequestBuilder restoreSnapshotRequestBuilder = clusterAdmin().prepareRestoreSnapshot(
                        TEST_REQUEST_TIMEOUT,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    if (restoreSpecificIndices) {
                        restoreSnapshotRequestBuilder.setIndices(indicesToRestore);
                    }

                    logger.info(
                        "--> restoring indices {} from [{}:{}]",
                        indicesToRestoreDescription,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    restoreSnapshotRequestBuilder.execute(mustSucceed(restoreSnapshotResponse -> {
                        logger.info(
                            "--> triggered restore of indices {} from [{}:{}], waiting for green health",
                            indicesToRestoreDescription,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );

                        prepareClusterHealthRequest(indicesToRestore).setWaitForGreenStatus()
                            .setWaitForNoInitializingShards(true)
                            .execute(mustSucceed(clusterHealthResponse -> {

                                logger.info(
                                    "--> indices {} successfully restored from [{}:{}]",
                                    indicesToRestoreDescription,
                                    snapshotInfo.repository(),
                                    snapshotInfo.snapshotId().getName()
                                );

                                Releasables.close(releaseAll);
                                assertFalse(clusterHealthResponse.isTimedOut());
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

                    if (localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (localReleasables.add(trackedSnapshot.tryAcquirePermit()) == null) {
                        return;
                    }

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final ListenableFuture<List<String>> getIndicesStep = new ListenableFuture<>();

                    logger.info(
                        "--> listing indices in [{}:{}] in preparation for cloning",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName
                    );

                    trackedSnapshot.getSnapshotInfo(client, mustSucceed(snapshotInfo -> {
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

                    getIndicesStep.addListener(mustSucceed(indexNames -> {

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
                            .prepareCloneSnapshot(
                                TEST_REQUEST_TIMEOUT,
                                trackedSnapshot.trackedRepository.repositoryName,
                                trackedSnapshot.snapshotName,
                                cloneName
                            )
                            .setIndices(indexNames.toArray(new String[0]))
                            .execute(mustSucceed(acknowledgedResponse -> {
                                Releasables.close(releaseAll);
                                assertTrue(acknowledgedResponse.isAcknowledged());
                                completedSnapshotLatch.countDown();
                                logger.info(
                                    "--> completed clone of [{}:{}] as [{}:{}]",
                                    trackedSnapshot.trackedRepository.repositoryName,
                                    trackedSnapshot.snapshotName,
                                    trackedSnapshot.trackedRepository.repositoryName,
                                    cloneName
                                );
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

                    if (localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final List<String> snapshotNames = new ArrayList<>();
                    final TrackedRepository targetRepository = blockSnapshotsFromOneRepository(localReleasables, snapshotNames);
                    if (targetRepository == null) return;

                    logger.info("--> starting deletion of [{}:{}]", targetRepository.repositoryName, snapshotNames);

                    final Releasable releaseAll = localReleasables.transfer();

                    client.admin()
                        .cluster()
                        .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, targetRepository.repositoryName, snapshotNames.toArray(new String[0]))
                        .execute(mustSucceed(acknowledgedResponse -> {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            for (String snapshotName : snapshotNames) {
                                assertThat(snapshots.remove(snapshotName), notNullValue());
                            }
                            Releasables.close(releaseAll); // must only release snapshot after removing it from snapshots map
                            logger.info("--> completed deletion of [{}:{}]", targetRepository.repositoryName, snapshotNames);
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
        private TrackedRepository blockSnapshotsFromOneRepository(TransferableReleasables localReleasables, List<String> snapshotNames) {
            final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
            TrackedRepository targetRepository = null;
            Randomness.shuffle(trackedSnapshots);
            for (TrackedSnapshot trackedSnapshot : trackedSnapshots) {
                if ((targetRepository == null || trackedSnapshot.trackedRepository == targetRepository)
                    && (snapshotNames.isEmpty() || randomBoolean())
                    && localReleasables.add(trackedSnapshot.tryAcquireAllPermits()) != null
                    && snapshots.get(trackedSnapshot.snapshotName) == trackedSnapshot) {

                    targetRepository = trackedSnapshot.trackedRepository;
                    snapshotNames.add(trackedSnapshot.snapshotName);
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

                    if (localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final Client client = localReleasables.add(acquireClient()).getClient();

                    for (TrackedRepository trackedRepository : repositories.values()) {
                        // cleanup forbids all concurrent snapshot activity
                        if (localReleasables.add(tryAcquireAllPermits(trackedRepository.permits)) == null) {
                            return;
                        }
                    }

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());

                    final Releasable releaseAll = localReleasables.transfer();

                    logger.info("--> starting cleanup of [{}]", trackedRepository.repositoryName);
                    client.admin()
                        .cluster()
                        .prepareCleanupRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, trackedRepository.repositoryName)
                        .execute(mustSucceed(cleanupRepositoryResponse -> {
                            final RepositoryCleanupResult result = cleanupRepositoryResponse.result();
                            if (result.bytes() > 0L || result.blobs() > 0L) {
                                // we could legitimately run into dangling blobs as the result of a shard snapshot failing half-way
                                // through the snapshot because of a concurrent index-close or -delete. The second round of cleanup on
                                // the same repository however should always find no more dangling blobs and be a no-op since we block all
                                // concurrent operations on the repository.
                                client.admin()
                                    .cluster()
                                    .prepareCleanupRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, trackedRepository.repositoryName)
                                    .execute(mustSucceed(secondCleanupRepositoryResponse -> {
                                        final RepositoryCleanupResult secondCleanupResult = secondCleanupRepositoryResponse.result();
                                        if (secondCleanupResult.blobs() == 1) {
                                            // The previous cleanup actually leaves behind a stale index-N blob, so this cleanup removes it
                                            // and reports it in its response. When https://github.com/elastic/elasticsearch/pull/100718 is
                                            // fixed the second cleanup will be a proper no-op and we can remove this lenience -- TODO
                                        } else {
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

                    if (releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually() && localReleasables.add(tryAcquirePermit(trackedIndex.permits)) != null) {
                            targetIndexNames.add(trackedIndex.indexName);
                        } else {
                            snapshotSpecificIndicesTmp = true;
                        }
                    }
                    final boolean snapshotSpecificIndices = snapshotSpecificIndicesTmp;

                    if (snapshotSpecificIndices && targetIndexNames.isEmpty()) {
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final ListenableFuture<ClusterHealthResponse> ensureYellowStep = new ListenableFuture<>();

                    final String snapshotName = "snapshot-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> waiting for yellow health of [{}] before creating snapshot [{}:{}]",
                        targetIndexNames,
                        trackedRepository.repositoryName,
                        snapshotName
                    );

                    prepareClusterHealthRequest(targetIndexNames.toArray(String[]::new)).setWaitForYellowStatus().execute(ensureYellowStep);

                    ensureYellowStep.addListener(mustSucceed(clusterHealthResponse -> {
                        assertFalse("timed out waiting for yellow state of " + targetIndexNames, clusterHealthResponse.isTimedOut());

                        logger.info(
                            "--> take snapshot [{}:{}] with indices [{}{}]",
                            trackedRepository.repositoryName,
                            snapshotName,
                            snapshotSpecificIndices ? "" : "*=",
                            targetIndexNames
                        );

                        final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = clusterAdmin().prepareCreateSnapshot(
                            TEST_REQUEST_TIMEOUT,
                            trackedRepository.repositoryName,
                            snapshotName
                        );

                        if (snapshotSpecificIndices) {
                            createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                        }

                        if (randomBoolean()) {
                            createSnapshotRequestBuilder.setWaitForCompletion(true);
                            createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                                logger.info("--> completed snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                                assertThat(stringFromSnapshotInfo(snapshotInfo), snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
                                Releasables.close(releaseAll);
                                completedSnapshotLatch.countDown();
                                startSnapshotter();
                            }));
                        } else {
                            createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                                logger.info("--> started snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                Releasables.close(releasableAfterStart.transfer());
                                pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                                    snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
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

                    if (releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually()
                            && releasableAfterStart.add(tryAcquirePermit(trackedIndex.permits)) != null
                            && localReleasables.add(trackedIndex.tryAcquirePartialSnapshottingPermit()) != null) {
                            targetIndexNames.add(trackedIndex.indexName);
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

                    final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = clusterAdmin().prepareCreateSnapshot(
                        TEST_REQUEST_TIMEOUT,
                        trackedRepository.repositoryName,
                        snapshotName
                    ).setPartial(true);

                    if (snapshotSpecificIndices) {
                        createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                    }

                    final boolean abortSnapshot = randomBoolean();
                    final Runnable abortRunnable;
                    if (abortSnapshot) {
                        try (TransferableReleasables abortReleasables = new TransferableReleasables()) {

                            assertNotNull(abortReleasables.add(blockFullClusterRestart()));
                            final Client abortClient = abortReleasables.add(acquireClient()).getClient();

                            assertNotNull(abortReleasables.add(tryAcquirePermit(trackedRepository.permits)));

                            final DeleteSnapshotRequestBuilder deleteSnapshotRequestBuilder = abortClient.admin()
                                .cluster()
                                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, trackedRepository.repositoryName, snapshotName);

                            final Releasable abortReleasable = abortReleasables.transfer();

                            abortRunnable = mustSucceed(() -> {
                                logger.info("--> abort/delete snapshot [{}:{}] start", trackedRepository.repositoryName, snapshotName);
                                deleteSnapshotRequestBuilder.execute(new ActionListener<>() {
                                    @Override
                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                        logger.info(
                                            "--> abort/delete snapshot [{}:{}] success",
                                            trackedRepository.repositoryName,
                                            snapshotName
                                        );
                                        Releasables.close(abortReleasable);
                                        assertTrue(acknowledgedResponse.isAcknowledged());
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        Releasables.close(abortReleasable);
                                        if (ExceptionsHelper.unwrapCause(e) instanceof SnapshotMissingException) {
                                            // processed before the snapshot even started
                                            logger.info(
                                                "--> abort/delete snapshot [{}:{}] got snapshot missing",
                                                trackedRepository.repositoryName,
                                                snapshotName
                                            );
                                        } else {
                                            logAndFailTest(e);
                                        }
                                    }
                                });
                            });
                        }
                    } else {
                        abortRunnable = () -> {};
                    }

                    createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                        logger.info("--> started partial snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                        Releasables.close(releasableAfterStart.transfer());
                        pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                            if (abortSnapshot == false) {
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
            clientExecutor.execute(
                mustSucceed(
                    () -> client.admin()
                        .cluster()
                        .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repositoryName)
                        .setCurrentSnapshot()
                        .execute(mustSucceed(getSnapshotsResponse -> {
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
                        if (localReleasables.add(tryAcquireAllPermits(trackedNode.permits)) != null) {

                            final String nodeName = trackedNode.nodeName;
                            final Releasable releaseAll = localReleasables.transfer();

                            threadPool.executor(NODE_RESTARTER).execute(mustSucceed(() -> {
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

        private void startNodeShutdownMarker() {
            enqueueAction(() -> {
                boolean rerun = true;
                if (usually()) {
                    return;
                }
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    if (localReleasables.add(blockFullClusterRestart()) == null) {
                        return;
                    }

                    final var node = randomFrom(shuffledNodes);

                    if (localReleasables.add(tryAcquirePermit(node.permits)) == null) {
                        return;
                    }

                    final var clusterService = cluster.getCurrentMasterNodeInstance(ClusterService.class);

                    if (node.nodeName.equals(clusterService.localNode().getName())) {
                        return;
                    }

                    logger.info("--> marking [{}] for removal", node);

                    SubscribableListener

                        .<Void>newForked(
                            l -> clusterService.submitUnbatchedStateUpdateTask(
                                "mark [" + node + "] for removal",
                                new ClusterStateUpdateTask() {
                                    @Override
                                    public ClusterState execute(ClusterState currentState) {
                                        assertTrue(
                                            Strings.toString(currentState),
                                            currentState.metadata().nodeShutdowns().getAll().isEmpty()
                                        );
                                        final var discoveryNode = currentState.nodes().resolveNode(node.nodeName);
                                        return currentState.copyAndUpdateMetadata(
                                            mdb -> mdb.putCustom(
                                                NodesShutdownMetadata.TYPE,
                                                new NodesShutdownMetadata(
                                                    Map.of(
                                                        discoveryNode.getId(),
                                                        SingleNodeShutdownMetadata.builder()
                                                            .setNodeId(discoveryNode.getId())
                                                            .setNodeEphemeralId(discoveryNode.getEphemeralId())
                                                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                                            .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                                                            .setReason("test")
                                                            .build()
                                                    )
                                                )
                                            )
                                        );
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        l.onFailure(e);
                                    }

                                    @Override
                                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                                        l.onResponse(null);
                                    }
                                }
                            )
                        )

                        .<Void>andThen(
                            l -> clusterService.submitUnbatchedStateUpdateTask(
                                "unmark [" + node + "] for removal",
                                new ClusterStateUpdateTask() {
                                    @Override
                                    public ClusterState execute(ClusterState currentState) {
                                        return currentState.copyAndUpdateMetadata(
                                            mdb -> mdb.putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY)
                                        );
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        l.onFailure(e);
                                    }

                                    @Override
                                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                                        l.onResponse(null);
                                        logger.info("--> unmarked [{}] for removal", node);
                                    }
                                }
                            )
                        )

                        .addListener(
                            ActionListener.releaseAfter(mustSucceed(ignored -> startNodeShutdownMarker()), localReleasables.transfer())
                        );

                    rerun = false;
                } finally {
                    if (rerun) {
                        startNodeShutdownMarker();
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

        /**
         * Acquire a client (i.e. block the client node from restarting) in a situation where we know that such a block can be obtained,
         * since previous acquisitions mean that at least one node is already blocked from restarting.
         */
        private ReleasableClient acquireClient() {
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

        /**
         * Tracks a repository in the cluster, and occasionally removes it and adds it back if no other activity holds any of its permits.
         */
        private class TrackedRepository {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String repositoryName;
            private final Path location;

            private TrackedRepository(String repositoryName, Path location) {
                this.repositoryName = repositoryName;
                this.location = location;
            }

            @Override
            public String toString() {
                return "TrackedRepository[" + repositoryName + "]";
            }

            public void start() {
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                    assertNotNull(localReleasables.add(blockNodeRestarts()));
                    assertNotNull(localReleasables.add(tryAcquireAllPermits(permits)));
                    final Client client = localReleasables.add(acquireClient()).getClient();
                    putRepositoryAndContinue(client, false, localReleasables.transfer());
                }
            }

            private void putRepositoryAndContinue(Client client, boolean nodeMightRestart, Releasable releasable) {
                logger.info("--> put repo [{}]", repositoryName);
                client.admin()
                    .cluster()
                    .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName)
                    .setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), location))
                    .setVerify(nodeMightRestart == false)
                    .execute(mustSucceed(acknowledgedResponse -> {
                        assertTrue(acknowledgedResponse.isAcknowledged());
                        logger.info("--> finished put repo [{}]", repositoryName);
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

                        if (localReleasables.add(tryAcquireAllPermits(permits)) == null) {
                            return;
                        }

                        if (localReleasables.add(blockFullClusterRestart()) == null) {
                            return;
                        }

                        final var nodeMightRestart = localReleasables.add(blockNodeRestarts()) == null;

                        final Client client = localReleasables.add(acquireClient()).getClient();

                        final Releasable releaseAll = localReleasables.transfer();

                        logger.info("--> delete repo [{}]", repositoryName);
                        clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName)
                            .execute(mustSucceed(acknowledgedResponse -> {
                                assertTrue(acknowledgedResponse.isAcknowledged());
                                logger.info("--> finished delete repo [{}]", repositoryName);
                                putRepositoryAndContinue(client, nodeMightRestart, releaseAll);
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
                    assertNotNull(localReleasables.add(blockNodeRestarts()));
                    assertNotNull(localReleasables.add(tryAcquireAllPermits(permits)));
                    createIndexAndContinue(localReleasables.transfer());
                }
            }

            private void createIndexAndContinue(Releasable releasable) {
                shardCount = between(1, 5);
                docPermits = new Semaphore(between(1000, 3000));
                logger.info("--> create index [{}] with max [{}] docs", indexName, docPermits.availablePermits());
                indicesAdmin().prepareCreate(indexName)
                    .setSettings(indexSettings(shardCount, between(0, cluster.numDataNodes() - 1)))
                    .execute(mustSucceed(response -> {
                        assertTrue(response.isAcknowledged());
                        logger.info("--> finished create index [{}]", indexName);
                        Releasables.close(releasable);
                        scheduleIndexingAndPossibleDelete();
                    }));
            }

            private void scheduleIndexingAndPossibleDelete() {
                enqueueAction(() -> {

                    boolean forked = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (localReleasables.add(blockNodeRestarts()) == null) {
                            return;
                        }

                        if (usually()) {
                            // index some more docs

                            if (localReleasables.add(tryAcquirePermit(permits)) == null) {
                                return;
                            }

                            final int maxDocCount = docPermits.drainPermits();
                            assert maxDocCount >= 0 : maxDocCount;
                            if (maxDocCount == 0) {
                                return;
                            }
                            final int docCount = between(1, Math.min(maxDocCount, 200));
                            docPermits.release(maxDocCount - docCount);

                            final Releasable releaseAll = localReleasables.transfer();

                            final ListenableFuture<ClusterHealthResponse> ensureYellowStep = new ListenableFuture<>();

                            logger.info("--> waiting for yellow health of [{}] prior to indexing [{}] docs", indexName, docCount);

                            prepareClusterHealthRequest(indexName).setWaitForYellowStatus().execute(ensureYellowStep);

                            final ListenableFuture<BulkResponse> bulkStep = new ListenableFuture<>();

                            ensureYellowStep.addListener(mustSucceed(clusterHealthResponse -> {

                                assertFalse(
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

                            bulkStep.addListener(mustSucceed(bulkItemResponses -> {
                                for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                                    assertNull(bulkItemResponse.getFailure());
                                }

                                logger.info("--> indexing into [{}] finished", indexName);

                                Releasables.close(releaseAll);
                                scheduleIndexingAndPossibleDelete();

                            }));

                            forked = true;

                        } else if (localReleasables.add(tryAcquireAllPermits(permits)) != null) {
                            // delete the index and create a new one

                            final Releasable releaseAll = localReleasables.transfer();

                            logger.info("--> deleting index [{}]", indexName);

                            indicesAdmin().prepareDelete(indexName).execute(mustSucceed(acknowledgedResponse -> {
                                logger.info("--> deleting index [{}] finished", indexName);
                                assertTrue(acknowledgedResponse.isAcknowledged());
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

            /**
             * We must not close an index while it's being partially snapshotted; this counter tracks the number of ongoing
             * close operations (positive) or partial snapshot operations (negative) in order to avoid them happening concurrently.
             * <p>
             * This is only a problem for partial snapshots because we release the index permit once a partial snapshot has started. With
             * non-partial snapshots we retain the index permit until it completes which blocks other operations.
             */
            private final AtomicInteger closingOrPartialSnapshottingCount = new AtomicInteger();

            private static boolean closingPermitAvailable(int value) {
                return value >= 0 && value != Integer.MAX_VALUE;
            }

            private static boolean partialSnapshottingPermitAvailable(int value) {
                return value <= 0 && value != Integer.MIN_VALUE;
            }

            Releasable tryAcquireClosingPermit() {
                final var previous = closingOrPartialSnapshottingCount.getAndUpdate(c -> closingPermitAvailable(c) ? c + 1 : c);
                if (closingPermitAvailable(previous)) {
                    return () -> assertThat(closingOrPartialSnapshottingCount.getAndDecrement(), greaterThan(0));
                } else {
                    return null;
                }
            }

            Releasable tryAcquirePartialSnapshottingPermit() {
                final var previous = closingOrPartialSnapshottingCount.getAndUpdate(c -> partialSnapshottingPermitAvailable(c) ? c - 1 : c);
                if (partialSnapshottingPermitAvailable(previous)) {
                    return () -> assertThat(closingOrPartialSnapshottingCount.getAndIncrement(), lessThan(0));
                } else {
                    return null;
                }
            }
        }

    }

    // Prepares a health request with twice the default (30s) timeout that waits for all cluster tasks to finish as well as all cluster
    // nodes before returning
    private static ClusterHealthRequestBuilder prepareClusterHealthRequest(String... targetIndexNames) {
        return clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, targetIndexNames)
            .setTimeout(TimeValue.timeValueSeconds(60))
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
            releasable.close();
        }

        Client getClient() {
            return client;
        }
    }

    /**
     * Tracks a snapshot taken by the cluster.
     */
    private static class TrackedSnapshot {

        private final TrackedCluster.TrackedRepository trackedRepository;
        private final String snapshotName;
        private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
        private final AtomicReference<SubscribableListener<SnapshotInfo>> snapshotInfoFutureRef = new AtomicReference<>();

        TrackedSnapshot(TrackedCluster.TrackedRepository trackedRepository, String snapshotName) {
            this.trackedRepository = trackedRepository;
            this.snapshotName = snapshotName;
        }

        /*
         * Try and acquire a permit on this snapshot and the underlying repository
         */
        Releasable tryAcquirePermit() {
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                if (localReleasables.add(SnapshotStressTestsIT.tryAcquirePermit(trackedRepository.permits)) == null) {
                    return null;
                }

                if (localReleasables.add(SnapshotStressTestsIT.tryAcquirePermit(permits)) == null) {
                    return null;
                }

                return localReleasables.transfer();
            }
        }

        Releasable tryAcquireAllPermits() {
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                if (localReleasables.add(SnapshotStressTestsIT.tryAcquirePermit(trackedRepository.permits)) == null) {
                    return null;
                }

                if (localReleasables.add(SnapshotStressTestsIT.tryAcquireAllPermits(permits)) == null) {
                    return null;
                }

                return localReleasables.transfer();
            }
        }

        void getSnapshotInfo(Client client, ActionListener<SnapshotInfo> listener) {
            final SubscribableListener<SnapshotInfo> newFuture = new SubscribableListener<>();

            final boolean firstRunner = snapshotInfoFutureRef.compareAndSet(null, newFuture);

            if (firstRunner == false) {
                if (usually()) {
                    snapshotInfoFutureRef.get().addListener(listener);
                    return;
                }
                // else (rarely) get it again, expecting it to be the same

                snapshotInfoFutureRef.get()
                    .addListener(
                        mustSucceed(
                            snapshotInfo1 -> newFuture.addListener(
                                mustSucceed(snapshotInfo2 -> assertThat(snapshotInfo1, equalTo(snapshotInfo2)))
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
                .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, trackedRepository.repositoryName)
                .setSnapshots(snapshotName)
                .execute(mustSucceed(getSnapshotsResponse -> {
                    assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
                    final SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);
                    assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName));
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
    private static class TrackedNode {

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
