/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotsServiceDoubleFinalizationIT extends AbstractSnapshotIntegTestCase {

    public void testNoDoubleFinalization() throws Exception {
        // 0 - Basic setup
        final String masterNodeName = internalCluster().startNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        createIndex(
            "index-1",
            Settings.builder().put("index.number_of_replicas", 0).put("index.routing.allocation.require._name", masterNodeName).build()
        );
        indexRandomDocs("index-1", 50);
        createIndex(
            "index-2",
            Settings.builder().put("index.number_of_replicas", 0).put("index.routing.allocation.require._name", dataNodeName).build()
        );
        indexRandomDocs("index-2", 50);
        createIndex(
            "index-3",
            Settings.builder().put("index.number_of_replicas", 0).put("index.routing.allocation.require._name", dataNodeName).build()
        );
        indexRandomDocs("index-3", 50);

        // 1 - create repository and take a snapshot
        final String repoName = "repo";
        createRepository(repoName, TestRepositoryPlugin.REPO_TYPE);
        final TestRepository testRepository = getRepositoryOnMaster(repoName);
        logger.info("--> create snapshot snap-1");
        createSnapshot(repoName, "snap-1", List.of("index-1"));

        // 2 - Start deleting the snap-1 and block it at listing root blobs
        PlainActionFuture<Void> future = setWaitForClusterState(state -> {
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = SnapshotDeletionsInProgress.get(state);
            return snapshotDeletionsInProgress.getEntries()
                .stream()
                .flatMap(entry -> entry.snapshots().stream())
                .anyMatch(snapshotId -> snapshotId.getName().equals("snap-1"));

        });
        final CyclicBarrier barrier = testRepository.blockOnceForListBlobs();
        new Thread(() -> {
            logger.info("--> start deleting snapshot snap-1 ");
            startDeleteSnapshot(repoName, "snap-1");
        }).start();
        assertBusy(() -> assertThat(barrier.getNumberWaiting(), equalTo(1)));
        future.actionGet();
        logger.info("--> repository blocked at listing root blobs");

        // 3 - Stop data node so that index-2, index-3 become unassigned
        internalCluster().stopNode(dataNodeName);
        internalCluster().validateClusterFormed();

        // 4 - Create new snapshot for the unassigned index and its shards should have both QUEUED and MISSING
        future = setWaitForClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
            return snapshotsInProgress.asStream()
                .anyMatch(
                    entry -> entry.snapshot().getSnapshotId().getName().equals("snap-2")
                        && entry.state() == SnapshotsInProgress.State.STARTED
                        && entry.shards()
                            .values()
                            .stream()
                            .map(SnapshotsInProgress.ShardSnapshotStatus::state)
                            .collect(Collectors.toSet())
                            .equals(Set.of(SnapshotsInProgress.ShardState.QUEUED, SnapshotsInProgress.ShardState.MISSING))
                );
        });
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-2")
            .setIndices("index-2", "index-3")
            .setPartial(true)
            .setWaitForCompletion(false)
            .get();
        // Delete index-3 so that it becomes MISSING for snapshot
        indicesAdmin().prepareDelete("index-3").get();
        future.actionGet();

        // 5 - Start deleting snap-2, itself should be WAITING. But changes InProgress snap-2 to SUCCESS
        future = setWaitForClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
            final boolean foundSnapshot = snapshotsInProgress.asStream()
                .anyMatch(
                    entry -> entry.snapshot().getSnapshotId().getName().equals("snap-2")
                        && entry.state() == SnapshotsInProgress.State.SUCCESS
                        && entry.shards()
                            .values()
                            .stream()
                            .map(SnapshotsInProgress.ShardSnapshotStatus::state)
                            .collect(Collectors.toSet())
                            .equals(Set.of(SnapshotsInProgress.ShardState.FAILED, SnapshotsInProgress.ShardState.MISSING))
                );
            if (false == foundSnapshot) {
                return false;
            }
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = SnapshotDeletionsInProgress.get(state);
            return snapshotDeletionsInProgress.getEntries()
                .stream()
                .anyMatch(
                    entry -> entry.state() == SnapshotDeletionsInProgress.State.WAITING
                        && entry.snapshots().stream().anyMatch(snapshotId -> snapshotId.getName().equals("snap-2"))
                );
        });
        new Thread(() -> {
            logger.info("--> start deleting snapshot snap-2 ");
            startDeleteSnapshot(repoName, "snap-2");
        }).start();
        future.actionGet();

        // 6 - Let the deletion of snap-1 to complete. It should *not* lead to double finalization
        barrier.await();

        awaitNoMoreRunningOperations();
    }

    private PlainActionFuture<Void> setWaitForClusterState(Predicate<ClusterState> predicate) {
        final var clusterStateObserver = new ClusterStateObserver(
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            TimeValue.timeValueMillis(60000),
            logger,
            new ThreadContext(Settings.EMPTY)
        );
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                future.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                future.onFailure(new IllegalStateException("cluster service closed"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                future.onFailure(new IllegalStateException("timeout"));
            }
        }, predicate, TimeValue.timeValueSeconds(30));
        return future;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestRepositoryPlugin.class);
    }

    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {

        public static final String REPO_TYPE = "test";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Map.of(
                REPO_TYPE,
                (projectId, metadata) -> new TestRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }
    }

    public static class TestRepository extends FsRepository {

        private static final Logger logger = LogManager.getLogger(TestRepository.class);
        private final AtomicReference<CyclicBarrier> barrierRef = new AtomicReference<>();

        public TestRepository(
            ProjectId projectId,
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(projectId, metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        public CyclicBarrier blockOnceForListBlobs() {
            final CyclicBarrier barrier = new CyclicBarrier(2);
            if (barrierRef.compareAndSet(null, barrier)) {
                return barrier;
            } else {
                throw new AssertionError("must unblock first");
            }
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            final var blobStore = super.createBlobStore();
            return new BlobStoreWrapper(blobStore) {

                @Override
                public BlobContainer blobContainer(BlobPath path) {
                    final var blobContainer = super.blobContainer(path);

                    return new FilterBlobContainer(blobContainer) {

                        @Override
                        protected BlobContainer wrapChild(BlobContainer child) {
                            return child;
                        }

                        @Override
                        public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
                            final CyclicBarrier barrier = barrierRef.get();
                            if (barrier != null) {
                                try {
                                    logger.info("--> Start blocking blobLists");
                                    barrier.await();
                                    if (false == barrierRef.compareAndSet(barrier, null)) {
                                        throw new AssertionError("barrier changed while blocking");
                                    }
                                    logger.info("--> Done blocking blobLists");
                                } catch (InterruptedException | BrokenBarrierException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            return super.listBlobs(purpose);
                        }
                    };
                }
            };
        }
    }
}
