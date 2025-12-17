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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotResiliencyTestHelper.TransportInterceptorFactory;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class SnapshotResiliencyTests extends ESTestCase {

    protected DeterministicTaskQueue deterministicTaskQueue;

    protected SnapshotResiliencyTestHelper.TestClusterNodes testClusterNodes;

    protected Path tempDir;

    @Before
    public void createServices() {
        tempDir = createTempDir();
        deterministicTaskQueue = createDeterministicTaskQueue();
    }

    protected DeterministicTaskQueue createDeterministicTaskQueue() {
        return new DeterministicTaskQueue();
    }

    @After
    public void verifyReposThenStopServices() throws ExecutionException {
        try {
            clearDisruptionsAndAwaitSync();

            final SubscribableListener<CleanupRepositoryResponse> cleanupResponse = new SubscribableListener<>();
            final SubscribableListener<CreateSnapshotResponse> createSnapshotResponse = new SubscribableListener<>();
            // Create another snapshot and then clean up the repository to verify that the repository works correctly no matter the
            // failures seen during the previous test.
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "last-snapshot")
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute(createSnapshotResponse);
            continueOrDie(createSnapshotResponse, r -> {
                final SnapshotInfo snapshotInfo = r.getSnapshotInfo();
                // Snapshot can be partial because some tests leave indices in a red state because data nodes were stopped
                assertThat(snapshotInfo.state(), either(is(SnapshotState.SUCCESS)).or(is(SnapshotState.PARTIAL)));
                assertThat(snapshotInfo.shardFailures(), iterableWithSize(snapshotInfo.failedShards()));
                assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards() - snapshotInfo.failedShards()));
                client().admin()
                    .cluster()
                    .cleanupRepository(new CleanupRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo"), cleanupResponse);
            });
            final AtomicBoolean cleanedUp = new AtomicBoolean(false);
            continueOrDie(cleanupResponse, r -> cleanedUp.set(true));

            runUntil(cleanedUp::get, TimeUnit.MINUTES.toMillis(1L));

            final PlainActionFuture<AssertionError> future = BlobStoreTestUtil.assertConsistencyAsync(
                (BlobStoreRepository) testClusterNodes.randomMasterNodeSafe().repositoriesService().repository("repo")
            );
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(future.isDone());
            final var result = future.result();
            if (result != null) {
                fail(result);
            }
        } finally {
            testClusterNodes.nodes().values().forEach(SnapshotResiliencyTestHelper.TestClusterNode::stop);
        }
    }

    public void testSuccessfulSnapshotAndRestore() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(0, 100);

        final SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseListener = new SubscribableListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final Runnable afterIndexing = () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseListener);
            if (documents == 0) {
                afterIndexing.run();
            } else {
                final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int i = 0; i < documents; ++i) {
                    bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
                }
                final SubscribableListener<BulkResponse> bulkResponseStepListener = new SubscribableListener<>();
                client().bulk(bulkRequest, bulkResponseStepListener);
                continueOrDie(bulkResponseStepListener, bulkResponse -> {
                    assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                    assertEquals(documents, bulkResponse.getItems().length);
                    afterIndexing.run();
                });
            }
        });

        final SubscribableListener<AcknowledgedResponse> deleteIndexListener = new SubscribableListener<>();

        continueOrDie(
            createSnapshotResponseListener,
            createSnapshotResponse -> client().admin().indices().delete(new DeleteIndexRequest(index), deleteIndexListener)
        );

        final SubscribableListener<SearchResponse> searchResponseListener = new SubscribableListener<>();
        continueOrDie(restoreSnapshotAndWaitForGreen(deleteIndexListener, repoName, snapshotName, index, shards), ignore -> {
            client().search(
                new SearchRequest(index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchResponseListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();
        continueOrDie(searchResponseListener, r -> {
            assertEquals(documents, Objects.requireNonNull(r.getHits().getTotalHits()).value());
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));
        assertNotNull(safeResult(createSnapshotResponseListener));
        assertTrue(documentCountVerified.get());
        assertTrue(SnapshotsInProgress.get(masterNode.clusterService().state()).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    private SnapshotInfo getSnapshotInfo(Repository repository, SnapshotId snapshotId) {
        final SubscribableListener<SnapshotInfo> listener = new SubscribableListener<>();
        repository.getSnapshotInfo(snapshotId, listener);
        deterministicTaskQueue.runAllRunnableTasks();
        return safeResult(listener);
    }

    public void testSnapshotWithNodeDisconnects() {
        final int dataNodes = randomIntBetween(2, 10);
        final int masterNodes = randomFrom(1, 3, 5);
        setupTestCluster(masterNodes, dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        final boolean partial = randomBoolean();
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                scheduleNow(this::disconnectRandomDataNode);
            }
            if (randomBoolean()) {
                scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
            }
            testClusterNodes.randomMasterNodeSafe()
                .client()
                .admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setPartial(partial)
                .execute(createSnapshotResponseStepListener);
        });

        final AtomicBoolean snapshotNeverStarted = new AtomicBoolean(false);

        createSnapshotResponseStepListener.addListener(ActionListener.wrap(createSnapshotResponse -> {
            for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                scheduleNow(this::disconnectOrRestartDataNode);
            }
            // Only disconnect master if we have more than a single master and can simulate a failover
            final boolean disconnectedMaster = randomBoolean() && masterNodes > 1;
            if (disconnectedMaster) {
                scheduleNow(this::disconnectOrRestartMasterNode);
            }
            if (disconnectedMaster || randomBoolean()) {
                scheduleSoon(() -> testClusterNodes.clearNetworkDisruptions());
            } else if (randomBoolean()) {
                scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
            }
        }, e -> {
            if (partial == false) {
                assertThat(
                    asInstanceOf(SnapshotException.class, ExceptionsHelper.unwrap(e, SnapshotException.class)).getMessage(),
                    allOf(
                        containsString("the following indices have unassigned primary shards"),
                        containsString("unless [partial] is set to [true]"),
                        containsString("[test]"),
                        containsString(ReferenceDocs.UNASSIGNED_SHARDS.toString())
                    )
                );
                snapshotNeverStarted.set(true);
            } else {
                throw new AssertionError(e);
            }
        }));

        runUntil(() -> testClusterNodes.randomMasterNode().map(master -> {
            if (snapshotNeverStarted.get()) {
                return true;
            }
            final SnapshotsInProgress snapshotsInProgress = master.clusterService().state().custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        final SnapshotResiliencyTestHelper.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        SnapshotsInProgress finalSnapshotsInProgress = SnapshotsInProgress.get(randomMaster.clusterService().state());
        assertTrue(finalSnapshotsInProgress.isEmpty());
        final Repository repository = randomMaster.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        if (snapshotNeverStarted.get()) {
            assertThat(snapshotIds, empty());
        } else {
            assertThat(snapshotIds, hasSize(1));
        }
    }

    public void testSnapshotDeleteWithMasterFailover() {
        final int dataNodes = randomIntBetween(2, 10);
        final int masterNodes = randomFrom(3, 5);
        setupTestCluster(masterNodes, dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        final boolean waitForSnapshot = randomBoolean();
        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> testClusterNodes.randomMasterNodeSafe()
                .client()
                .admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(waitForSnapshot)
                .execute(createSnapshotResponseStepListener)
        );

        final AtomicBoolean snapshotDeleteResponded = new AtomicBoolean(false);
        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(this::disconnectOrRestartMasterNode);
            testClusterNodes.randomDataNodeSafe()
                .client()
                .admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .execute(ActionListener.running(() -> snapshotDeleteResponded.set(true)));
        });

        runUntil(
            () -> testClusterNodes.randomMasterNode()
                .map(
                    master -> snapshotDeleteResponded.get()
                        && SnapshotDeletionsInProgress.get(master.clusterService().state()).getEntries().isEmpty()
                )
                .orElse(false),
            TimeUnit.MINUTES.toMillis(1L)
        );

        clearDisruptionsAndAwaitSync();

        final SnapshotResiliencyTestHelper.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        assertTrue(SnapshotsInProgress.get(randomMaster.clusterService().state()).isEmpty());
        final Repository repository = randomMaster.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(0));
    }

    public void testConcurrentSnapshotCreateAndDelete() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .execute(createSnapshotResponseStepListener)
        );

        final SubscribableListener<AcknowledgedResponse> deleteSnapshotStepListener = new SubscribableListener<>();

        masterNode.clusterService().addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (SnapshotsInProgress.get(event.state()).isEmpty() == false) {
                    client().admin()
                        .cluster()
                        .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                        .execute(deleteSnapshotStepListener);
                    masterNode.clusterService().removeListener(this);
                }
            }
        });

        final SubscribableListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            deleteSnapshotStepListener,
            acknowledgedResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener)
        );
        continueOrDie(
            createAnotherSnapshotResponseStepListener,
            createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        assertNotNull(safeResult(createSnapshotResponseStepListener));
        assertNotNull(safeResult(createAnotherSnapshotResponseStepListener));
        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testConcurrentSnapshotCreateAndDeleteOther() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final SubscribableListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-2")
                .execute(createOtherSnapshotResponseStepListener)
        );

        final SubscribableListener<AcknowledgedResponse> deleteSnapshotStepListener = new SubscribableListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .execute(deleteSnapshotStepListener)
        );

        final SubscribableListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(deleteSnapshotStepListener, deleted -> {
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener);
            continueOrDie(
                createAnotherSnapshotResponseStepListener,
                createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        // We end up with two snapshots no matter if the delete worked out or not
        assertThat(snapshotIds, hasSize(2));

        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    public void testBulkSnapshotDeleteWithAbort() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final int inProgressSnapshots = randomIntBetween(1, 5);
        final var createOtherSnapshotResponseStepListener = new SubscribableListener<Collection<CreateSnapshotResponse>>();
        final ActionListener<CreateSnapshotResponse> createSnapshotListener = new GroupedActionListener<>(
            inProgressSnapshots,
            createOtherSnapshotResponseStepListener
        );

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (int i = 0; i < inProgressSnapshots; i++) {
                client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "other-" + i)
                    .execute(createSnapshotListener);
            }
        });

        final SubscribableListener<AcknowledgedResponse> deleteSnapshotStepListener = new SubscribableListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .deleteSnapshot(new DeleteSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, "*"), deleteSnapshotStepListener)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        // No snapshots should be left in the repository
        assertThat(snapshotIds, empty());
    }

    public void testConcurrentSnapshotRestoreAndDeleteOther() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        final int documentsFirstSnapshot = randomIntBetween(0, 100);

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> indexNDocuments(
                documentsFirstSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setWaitForCompletion(true)
                    .execute(createSnapshotResponseStepListener)
            )
        );

        final int documentsSecondSnapshot = randomIntBetween(0, 100);

        final SubscribableListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new SubscribableListener<>();

        final String secondSnapshotName = "snapshot-2";
        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> indexNDocuments(
                documentsSecondSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, secondSnapshotName)
                    .setWaitForCompletion(true)
                    .execute(createOtherSnapshotResponseStepListener)
            )
        );

        final SubscribableListener<AcknowledgedResponse> deleteSnapshotStepListener = new SubscribableListener<>();
        final SubscribableListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new SubscribableListener<>();

        continueOrDie(createOtherSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(
                () -> client().admin()
                    .cluster()
                    .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .execute(deleteSnapshotStepListener)
            );
            scheduleNow(
                () -> client().admin()
                    .cluster()
                    .restoreSnapshot(
                        new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, secondSnapshotName).waitForCompletion(true)
                            .renamePattern("(.+)")
                            .renameReplacement("restored_$1"),
                        restoreSnapshotResponseListener
                    )
            );
        });

        final var restoredIndexGreenListener = waitForGreenAfterRestore(restoreSnapshotResponseListener, "restored_" + index, shards);

        final SubscribableListener<SearchResponse> searchResponseListener = new SubscribableListener<>();
        continueOrDie(restoredIndexGreenListener, ignored -> {
            client().search(
                new SearchRequest("restored_" + index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchResponseListener.delegateFailure((l, r) -> {
                    r.incRef();
                    l.onResponse(r);
                })
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        var response = safeResult(searchResponseListener);
        try {
            assertEquals(
                documentsFirstSnapshot + documentsSecondSnapshot,
                Objects.requireNonNull(response.getHits().getTotalHits()).value()
            );
        } finally {
            response.decRef();
        }

        assertThat(safeResult(deleteSnapshotStepListener).isAcknowledged(), is(true));
        assertThat(safeResult(restoreSnapshotResponseListener).getRestoreInfo().failedShards(), is(0));

        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, contains(safeResult(createOtherSnapshotResponseStepListener).getSnapshotInfo().snapshotId()));

        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    private void indexNDocuments(int documents, String index, Runnable afterIndexing) {
        if (documents == 0) {
            afterIndexing.run();
            return;
        }
        final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < documents; ++i) {
            bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
        }
        final SubscribableListener<BulkResponse> bulkResponseStepListener = new SubscribableListener<>();
        client().bulk(bulkRequest, bulkResponseStepListener);
        continueOrDie(bulkResponseStepListener, bulkResponse -> {
            assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
            assertEquals(documents, bulkResponse.getItems().length);
            afterIndexing.run();
        });
    }

    public void testConcurrentSnapshotDeleteAndDeleteIndex() throws IOException {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<Collection<CreateIndexResponse>> createIndicesListener = new SubscribableListener<>();
        final int indices = randomIntBetween(5, 20);

        final SetOnce<Index> firstIndex = new SetOnce<>();
        continueOrDie(createRepoAndIndex(repoName, index, 1), createIndexResponse -> {
            firstIndex.set(masterNode.clusterService().state().metadata().getProject().index(index).getIndex());
            // create a few more indices to make it more likely that the subsequent index delete operation happens before snapshot
            // finalization
            final GroupedActionListener<CreateIndexResponse> listener = new GroupedActionListener<>(indices, createIndicesListener);
            for (int i = 0; i < indices; ++i) {
                client().admin().indices().create(new CreateIndexRequest("index-" + i).settings(defaultIndexSettings(1)), listener);
            }
        });

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        final boolean partialSnapshot = randomBoolean();

        continueOrDie(
            createIndicesListener,
            createIndexResponses -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(false)
                .setPartial(partialSnapshot)
                .setIncludeGlobalState(randomBoolean())
                .execute(createSnapshotResponseStepListener)
        );

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin().indices().delete(new DeleteIndexRequest(index), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (partialSnapshot) {
                        // Recreate index by the same name to test that we don't snapshot conflicting metadata in this scenario
                        client().admin()
                            .indices()
                            .create(new CreateIndexRequest(index).settings(defaultIndexSettings(1)), ActionListener.noop());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (partialSnapshot) {
                        throw new AssertionError("Delete index should always work during partial snapshots", e);
                    }
                }
            })
        );

        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotIds.iterator().next());
        if (partialSnapshot) {
            assertThat(snapshotInfo.state(), either(is(SnapshotState.SUCCESS)).or(is(SnapshotState.PARTIAL)));
            // Single shard for each index so we either get all indices or all except for the deleted index
            assertThat(snapshotInfo.successfulShards(), either(is(indices + 1)).or(is(indices)));
            if (snapshotInfo.successfulShards() == indices + 1) {
                final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(
                    repositoryData,
                    snapshotInfo.snapshotId(),
                    repositoryData.resolveIndexId(index)
                );
                // Make sure we snapshotted the metadata of this index and not the recreated version
                assertEquals(indexMetadata.getIndex(), firstIndex.get());
            }
        } else {
            assertEquals(snapshotInfo.state(), SnapshotState.SUCCESS);
            // Index delete must be blocked for non-partial snapshots and we get a snapshot for every index
            assertEquals(snapshotInfo.successfulShards(), indices + 1);
        }
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testConcurrentDeletes() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final Collection<SubscribableListener<Boolean>> deleteSnapshotStepListeners = List.of(
            new SubscribableListener<>(),
            new SubscribableListener<>()
        );

        final AtomicInteger successfulDeletes = new AtomicInteger(0);

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (SubscribableListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
                client().admin()
                    .cluster()
                    .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .execute(ActionListener.wrap(resp -> deleteListener.onResponse(true), e -> {
                        final Throwable unwrapped = ExceptionsHelper.unwrap(
                            e,
                            ConcurrentSnapshotExecutionException.class,
                            SnapshotMissingException.class
                        );
                        assertThat(unwrapped, notNullValue());
                        deleteListener.onResponse(false);
                    }));
            }
        });

        for (SubscribableListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
            continueOrDie(deleteListener, deleted -> {
                if (deleted) {
                    successfulDeletes.incrementAndGet();
                }
            });
        }

        deterministicTaskQueue.runAllRunnableTasks();

        assertFalse(SnapshotDeletionsInProgress.get(masterNode.clusterService().state()).hasDeletionsInProgress());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        // We end up with no snapshots since at least one of the deletes worked out
        assertThat(snapshotIds, empty());
        assertThat(successfulDeletes.get(), either(is(1)).or(is(2)));
        // We did one snapshot and one delete so we went two steps from the empty generation (-1) to 1
        assertThat(repositoryData.getGenId(), is(1L));
    }

    /**
     * Simulates concurrent restarts of data and master nodes as well as relocating a primary shard, while starting and subsequently
     * deleting a snapshot.
     */
    public void testSnapshotPrimaryRelocations() {
        final int masterNodeCount = randomFrom(1, 3, 5);
        setupTestCluster(masterNodeCount, randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);

        final SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        final AdminClient masterAdminClient = masterNode.client().admin();

        final SubscribableListener<ClusterStateResponse> clusterStateResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT), clusterStateResponseStepListener)
        );

        continueOrDie(clusterStateResponseStepListener, clusterStateResponse -> {
            final ShardRouting shardToRelocate = clusterStateResponse.getState()
                .routingTable()
                .allShards(index)
                .stream()
                .filter(ShardRouting::primary)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no primary shard found"));
            final SnapshotResiliencyTestHelper.TestClusterNode currentPrimaryNode = testClusterNodes.nodeById(
                shardToRelocate.currentNodeId()
            );
            final SnapshotResiliencyTestHelper.TestClusterNode otherNode = testClusterNodes.randomDataNodeSafe(
                currentPrimaryNode.node().getName()
            );
            scheduleNow(() -> testClusterNodes.stopNode(currentPrimaryNode));
            scheduleNow(new Runnable() {
                @Override
                public void run() {
                    final SubscribableListener<ClusterStateResponse> updatedClusterStateResponseStepListener = new SubscribableListener<>();
                    masterAdminClient.cluster()
                        .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT), updatedClusterStateResponseStepListener);
                    continueOrDie(updatedClusterStateResponseStepListener, updatedClusterState -> {
                        final ShardRouting shardRouting = updatedClusterState.getState()
                            .routingTable()
                            .shardRoutingTable(shardToRelocate.shardId())
                            .primaryShard();
                        if (shardRouting.unassigned() && shardRouting.unassignedInfo().reason() == UnassignedInfo.Reason.NODE_LEFT) {
                            if (masterNodeCount > 1) {
                                scheduleNow(() -> testClusterNodes.stopNode(masterNode));
                            }
                            testClusterNodes.randomDataNodeSafe()
                                .client()
                                .admin()
                                .cluster()
                                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                                .execute(ActionListener.running(() -> {
                                    createdSnapshot.set(true);
                                    testClusterNodes.randomDataNodeSafe()
                                        .client()
                                        .admin()
                                        .cluster()
                                        .deleteSnapshot(
                                            new DeleteSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName),
                                            ActionListener.noop()
                                        );
                                }));
                            scheduleNow(
                                () -> testClusterNodes.randomMasterNodeSafe()
                                    .client()
                                    .execute(
                                        TransportClusterRerouteAction.TYPE,
                                        new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(
                                            new AllocateEmptyPrimaryAllocationCommand(
                                                index,
                                                shardRouting.shardId().id(),
                                                otherNode.node().getName(),
                                                true
                                            )
                                        ),
                                        ActionListener.noop()
                                    )
                            );
                        } else if (shardRouting.started()
                            && shardRouting.currentNodeId().equals(currentPrimaryNode.node().getId()) == false) {
                                assertTrue(DiscoveryNode.isStateless(masterNode.settings));
                                if (masterNodeCount > 1) {
                                    scheduleNow(() -> testClusterNodes.stopNode(masterNode));
                                }
                                testClusterNodes.randomDataNodeSafe()
                                    .client()
                                    .admin()
                                    .cluster()
                                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                                    .execute(ActionListener.running(() -> {
                                        createdSnapshot.set(true);
                                        testClusterNodes.randomDataNodeSafe()
                                            .client()
                                            .admin()
                                            .cluster()
                                            .deleteSnapshot(
                                                new DeleteSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName),
                                                ActionListener.noop()
                                            );
                                    }));
                            } else {
                                scheduleSoon(this);
                            }
                    });
                }
            });
        });

        runUntil(() -> testClusterNodes.randomMasterNode().map(master -> {
            if (createdSnapshot.get() == false) {
                return false;
            }
            return SnapshotsInProgress.get(master.clusterService().state()).isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        assertTrue(SnapshotsInProgress.get(testClusterNodes.randomDataNodeSafe().clusterService().state()).isEmpty());
        final Repository repository = testClusterNodes.randomMasterNodeSafe().repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, either(hasSize(1)).or(hasSize(0)));
    }

    public void testSuccessfulSnapshotWithConcurrentDynamicMappingUpdates() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(2, 100);
        SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final AtomicBoolean initiatedSnapshot = new AtomicBoolean(false);
            for (int i = 0; i < documents; ++i) {
                // Index a few documents with different field names so we trigger a dynamic mapping update for each of them
                client().bulk(
                    new BulkRequest().add(new IndexRequest(index).source(Map.of("foo" + i, "bar")))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    assertNoFailureListener(bulkResponse -> {
                        assertFalse("Failures in bulkresponse: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                        if (initiatedSnapshot.compareAndSet(false, true)) {
                            client().admin()
                                .cluster()
                                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                                .setWaitForCompletion(true)
                                .execute(createSnapshotResponseStepListener);
                        }
                    })
                );
            }
        });

        final String restoredIndex = "restored";

        final SubscribableListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).renamePattern(index)
                        .renameReplacement(restoredIndex)
                        .waitForCompletion(true),
                    restoreSnapshotResponseStepListener
                )
        );

        final var restoredIndexGreenListener = waitForGreenAfterRestore(restoreSnapshotResponseStepListener, restoredIndex, shards);

        final SubscribableListener<SearchResponse> searchResponseStepListener = new SubscribableListener<>();

        continueOrDie(restoredIndexGreenListener, restoreSnapshotResponse -> {
            client().search(
                new SearchRequest(restoredIndex).source(new SearchSourceBuilder().size(documents).trackTotalHits(true)),
                searchResponseStepListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();

        continueOrDie(searchResponseStepListener, r -> {
            final long hitCount = r.getHits().getTotalHits().value();
            assertThat(
                "Documents were restored but the restored index mapping was older than some documents and misses some of their fields",
                (int) hitCount,
                lessThanOrEqualTo(
                    ((Map<?, ?>) masterNode.clusterService()
                        .state()
                        .metadata()
                        .getProject()
                        .index(restoredIndex)
                        .mapping()
                        .sourceAsMap()
                        .get("properties")).size()
                )
            );
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));

        assertNotNull(safeResult(createSnapshotResponseStepListener));
        assertNotNull(safeResult(restoreSnapshotResponseStepListener));
        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testRunConcurrentSnapshots() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        final String repoName = "repo";
        final List<String> snapshotNames = IntStream.range(1, randomIntBetween(2, 4)).mapToObj(i -> "snapshot-" + i).toList();
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(1, 100);

        final SnapshotResiliencyTestHelper.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes().values().iterator().next().clusterService().state()
        );

        final SubscribableListener<Collection<CreateSnapshotResponse>> allSnapshotsListener = new SubscribableListener<>();
        final ActionListener<CreateSnapshotResponse> snapshotListener = new GroupedActionListener<>(
            snapshotNames.size(),
            allSnapshotsListener
        );
        final AtomicBoolean doneIndexing = new AtomicBoolean(false);
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (String snapshotName : snapshotNames) {
                scheduleNow(
                    () -> client().admin()
                        .cluster()
                        .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                        .setWaitForCompletion(true)
                        .execute(snapshotListener)
                );
            }
            final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < documents; ++i) {
                bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
            }
            final SubscribableListener<BulkResponse> bulkResponseStepListener = new SubscribableListener<>();
            client().bulk(bulkRequest, bulkResponseStepListener);
            continueOrDie(bulkResponseStepListener, bulkResponse -> {
                assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                assertEquals(documents, bulkResponse.getItems().length);
                doneIndexing.set(true);
            });
        });

        final AtomicBoolean doneSnapshotting = new AtomicBoolean(false);
        continueOrDie(allSnapshotsListener, createSnapshotResponses -> {
            for (CreateSnapshotResponse createSnapshotResponse : createSnapshotResponses) {
                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
            }
            doneSnapshotting.set(true);
        });

        runUntil(() -> doneIndexing.get() && doneSnapshotting.get(), TimeUnit.MINUTES.toMillis(5L));
        assertTrue(masterNode.clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
        final Repository repository = masterNode.repositoriesService().repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(snapshotNames.size()));

        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    public void testSnapshotCompletedByNodeLeft() {

        // A transport interceptor that throttles the shard snapshot status updates to run one at a time, for more interesting interleavings
        final TransportInterceptor throttlingInterceptor = new TransportInterceptor() {
            private final ThrottledTaskRunner runner = new ThrottledTaskRunner(
                TransportUpdateSnapshotStatusAction.NAME + "-throttle",
                1,
                SnapshotResiliencyTests.this::scheduleNow
            );

            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                Executor executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
                if (action.equals(TransportUpdateSnapshotStatusAction.NAME)) {
                    return (request, channel, task) -> ActionListener.run(
                        new ChannelActionListener<>(channel),
                        l -> runner.enqueueTask(
                            l.delegateFailureAndWrap(
                                (ll, ref) -> actualHandler.messageReceived(
                                    request,
                                    new TestTransportChannel(ActionListener.releaseAfter(ll, ref)),
                                    task
                                )
                            )
                        )
                    );
                } else {
                    return actualHandler;
                }
            }
        };

        setupTestCluster(1, 1, node -> node.isMasterNode() ? throttlingInterceptor : TransportService.NOOP_TRANSPORT_INTERCEPTOR);

        final var masterNode = testClusterNodes.randomMasterNodeSafe();
        final var client = masterNode.client();
        final var masterClusterService = masterNode.clusterService();

        final var indices = IntStream.range(0, between(1, 4)).mapToObj(i -> "index-" + i).toList();
        final var repoName = "repo";
        final var originalSnapshotName = "original-snapshot";

        var testListener = SubscribableListener

            // Create the repo and indices
            .<Void>newForked(stepListener -> {
                try (var listeners = new RefCountingListener(stepListener)) {
                    client().admin()
                        .cluster()
                        .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                        .setType(FsRepository.TYPE)
                        .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
                        .execute(listeners.acquire(createRepoResponse -> {}));

                    for (final var index : indices) {
                        client.admin()
                            .indices()
                            .create(
                                new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL).settings(defaultIndexSettings(1)),
                                listeners.acquire(createIndexResponse -> {})
                            );
                    }
                }
            })

            // Take a full snapshot for use as the source for future clones
            .<Void>andThen(
                (l, ignored) -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, originalSnapshotName)
                    .setWaitForCompletion(true)
                    .execute(l.map(v -> null))
            );

        final var snapshotCount = between(1, 10);
        for (int i = 0; i < snapshotCount; i++) {
            // Launch a random set of snapshots and clones, one at a time for more interesting interleavings
            if (randomBoolean()) {
                final var snapshotName = "snapshot-" + i;
                testListener = testListener.andThen(
                    stepListener -> scheduleNow(
                        ActionRunnable.wrap(
                            stepListener,
                            l -> client.admin()
                                .cluster()
                                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                                .setIndices(randomNonEmptySubsetOf(indices).toArray(String[]::new))
                                .setPartial(true)
                                .execute(l.map(v1 -> null))
                        )
                    )
                );
            } else {
                final var cloneName = "clone-" + i;
                testListener = testListener.andThen(stepListener -> scheduleNow(ActionRunnable.wrap(stepListener, l -> {
                    // The clone API only responds when the clone is complete, but we only want to wait until the clone starts so we watch
                    // the cluster state instead.
                    ClusterServiceUtils.addTemporaryStateListener(
                        masterClusterService,
                        cs -> SnapshotsInProgress.get(cs)
                            .forRepo(repoName)
                            .stream()
                            .anyMatch(
                                e -> e.snapshot().getSnapshotId().getName().equals(cloneName)
                                    && e.isClone()
                                    && e.shardSnapshotStatusByRepoShardId().isEmpty() == false
                            )
                    ).addListener(l);
                    client.admin()
                        .cluster()
                        .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, originalSnapshotName, cloneName)
                        .setIndices(randomNonEmptySubsetOf(indices).toArray(String[]::new))
                        .execute(ActionTestUtils.assertNoFailureListener(r -> {}));
                })));
            }
        }

        testListener = testListener.andThen(l -> scheduleNow(() -> {
            // Once all snapshots & clones have started, drop the data node and wait for all snapshot activity to complete
            testClusterNodes.disconnectNode(testClusterNodes.randomDataNodeSafe());
            ClusterServiceUtils.addTemporaryStateListener(masterClusterService, cs -> SnapshotsInProgress.get(cs).isEmpty()).addListener(l);
        }));

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(
            "executed all runnable tasks but test steps are still incomplete: "
                + Strings.toString(SnapshotsInProgress.get(masterClusterService.state()), true, true),
            testListener.isDone()
        );
        safeAwait(testListener); // shouldn't throw
    }

    /**
     * A device for allowing a test precise control over the order in which shard-snapshot updates are processed by the master. The test
     * must install the result of {@link #newTransportInterceptor} on the master node and may then call {@link #releaseBlock} as needed to
     * release blocks on the processing of individual shard snapshot updates.
     */
    private static class ShardSnapshotUpdatesSequencer {

        private final Map<String, Map<String, SubscribableListener<Void>>> shardSnapshotUpdatesBlockMap = new HashMap<>();

        private static final SubscribableListener<Void> ALWAYS_PROCEED = SubscribableListener.newSucceeded(null);

        private SubscribableListener<Void> listenerFor(String snapshot, String index) {
            if ("last-snapshot".equals(snapshot) || "first-snapshot".equals(snapshot)) {
                return ALWAYS_PROCEED;
            }

            return shardSnapshotUpdatesBlockMap
                //
                .computeIfAbsent(snapshot, v -> new HashMap<>())
                .computeIfAbsent(index, v -> new SubscribableListener<>());
        }

        void releaseBlock(String snapshot, String index) {
            listenerFor(snapshot, index).onResponse(null);
        }

        /**
         * @return a {@link TransportInterceptor} which enforces the sequencing of shard snapshot updates
         */
        TransportInterceptor newTransportInterceptor() {
            return new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (action.equals(TransportUpdateSnapshotStatusAction.NAME)) {
                        return (request, channel, task) -> ActionListener.run(
                            ActionTestUtils.<TransportResponse>assertNoFailureListener(new ChannelActionListener<>(channel)::onResponse),
                            l -> {
                                final var updateRequest = asInstanceOf(UpdateIndexShardSnapshotStatusRequest.class, request);
                                listenerFor(updateRequest.snapshot().getSnapshotId().getName(), updateRequest.shardId().getIndexName()).<
                                    TransportResponse>andThen(
                                        ll -> actualHandler.messageReceived(request, new TestTransportChannel(ll), task)
                                    ).addListener(l);
                            }
                        );
                    } else {
                        return actualHandler;
                    }
                }
            };
        }
    }

    public void testDeleteIndexBetweenSuccessAndFinalization() {

        final var sequencer = new ShardSnapshotUpdatesSequencer();

        setupTestCluster(
            1,
            1,
            node -> node.isMasterNode() ? sequencer.newTransportInterceptor() : TransportService.NOOP_TRANSPORT_INTERCEPTOR
        );

        final var masterNode = testClusterNodes.randomMasterNodeSafe();
        final var client = masterNode.client();
        final var masterClusterService = masterNode.clusterService();

        final var snapshotCount = between(3, 5);
        final var indices = IntStream.range(0, snapshotCount + 1).mapToObj(i -> "index-" + i).toList();
        final var repoName = "repo";
        final var indexToDelete = "index-" + snapshotCount;

        var testListener = SubscribableListener

            // Create the repo and indices
            .<Void>newForked(stepListener -> {
                try (var listeners = new RefCountingListener(stepListener)) {
                    client().admin()
                        .cluster()
                        .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                        .setType(FsRepository.TYPE)
                        .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
                        .execute(listeners.acquire(createRepoResponse -> {}));

                    for (final var index : indices) {
                        client.admin()
                            .indices()
                            .create(
                                new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL).settings(defaultIndexSettings(1)),
                                listeners.acquire(createIndexResponse -> {})
                            );
                    }
                }
            })
            .andThen(l -> {
                // Create the first snapshot as source of the clone
                client.admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first-snapshot")
                    .setIndices("index-0", indexToDelete)
                    .setPartial(false)
                    .setWaitForCompletion(true)
                    .execute(l.map(v -> null));
            });

        // Start some snapshots such that snapshot-{i} contains index-{i} and index-{snapshotCount} so that we can control the order in
        // which they finalize by controlling the order in which the shard snapshot updates are processed
        final var cloneFuture = new PlainActionFuture<AcknowledgedResponse>();
        for (int i = 0; i < snapshotCount; i++) {
            final var snapshotName = "snapshot-" + i;
            final var indexName = "index-" + i;
            testListener = testListener.andThen(
                stepListener -> client.admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setIndices(indexName, indexToDelete)
                    .setPartial(true)
                    .execute(stepListener.map(createSnapshotResponse -> null))
            );
            if (i == 0) {
                // Insert a clone between snapshot-0 and snapshot-1 and it finalizes after snapshot-1 because it will be blocked on index-0
                testListener = testListener.andThen(stepListener -> {
                    client.admin()
                        .cluster()
                        .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first-snapshot", "clone")
                        .setIndices("index-0", indexToDelete)
                        .execute(cloneFuture);
                    ClusterServiceUtils.addTemporaryStateListener(
                        masterClusterService,
                        clusterState -> SnapshotsInProgress.get(clusterState)
                            .asStream()
                            .anyMatch(e -> e.snapshot().getSnapshotId().getName().equals("clone") && e.isClone())
                    ).addListener(stepListener.map(v -> null));
                });
            }
        }

        testListener = testListener
            // wait for the target index to complete in snapshot-1
            .andThen(l -> {
                sequencer.releaseBlock("snapshot-0", indexToDelete);
                sequencer.releaseBlock("snapshot-1", indexToDelete);

                ClusterServiceUtils.addTemporaryStateListener(
                    masterClusterService,
                    clusterState -> SnapshotsInProgress.get(clusterState)
                        .asStream()
                        .filter(e -> e.isClone() == false)
                        .mapToLong(
                            e -> e.shards()
                                .entrySet()
                                .stream()
                                .filter(
                                    e2 -> e2.getKey().getIndexName().equals(indexToDelete)
                                        && e2.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS
                                )
                                .count()
                        )
                        .sum() == 2
                ).addListener(l.map(v -> null));
            })

            // delete the target index
            .andThen(l -> client.admin().indices().delete(new DeleteIndexRequest(indexToDelete), l.map(acknowledgedResponse -> null)))

            // wait for snapshot-1 to complete
            .andThen(l -> {
                sequencer.releaseBlock("snapshot-1", "index-1");
                ClusterServiceUtils.addTemporaryStateListener(
                    masterClusterService,
                    cs -> SnapshotsInProgress.get(cs).asStream().noneMatch(e -> e.snapshot().getSnapshotId().getName().equals("snapshot-1"))
                ).addListener(l.map(v -> null));
            })

            // wait for all the other snapshots to complete
            .andThen(l -> {
                // Clone is yet to be finalized
                assertTrue(SnapshotsInProgress.get(masterClusterService.state()).asStream().anyMatch(SnapshotsInProgress.Entry::isClone));
                for (int i = 0; i < snapshotCount; i++) {
                    sequencer.releaseBlock("snapshot-" + i, indexToDelete);
                    sequencer.releaseBlock("snapshot-" + i, "index-" + i);
                }
                ClusterServiceUtils.addTemporaryStateListener(masterClusterService, cs -> SnapshotsInProgress.get(cs).isEmpty())
                    .addListener(l.map(v -> null));
            })
            .andThen(l -> {
                final var snapshotNames = Stream.concat(
                    Stream.of("clone"),
                    IntStream.range(0, snapshotCount).mapToObj(i -> "snapshot-" + i)
                ).toArray(String[]::new);

                client.admin()
                    .cluster()
                    .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName)
                    .setSnapshots(snapshotNames)
                    .execute(ActionTestUtils.assertNoFailureListener(getSnapshotsResponse -> {
                        for (final var snapshot : getSnapshotsResponse.getSnapshots()) {
                            assertThat(snapshot.state(), is(SnapshotState.SUCCESS));
                            final String snapshotName = snapshot.snapshot().getSnapshotId().getName();
                            if ("clone".equals(snapshotName)) {
                                // Clone is not affected by index deletion
                                assertThat(snapshot.indices(), containsInAnyOrder("index-0", indexToDelete));
                            } else {
                                // Does not contain the deleted index in the snapshot
                                assertThat(snapshot.indices(), contains("index-" + snapshotName.charAt(snapshotName.length() - 1)));
                            }
                        }
                        l.onResponse(null);
                    }));
            });

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(
            "executed all runnable tasks but test steps are still incomplete: "
                + Strings.toString(SnapshotsInProgress.get(masterClusterService.state()), true, true),
            testListener.isDone()
        );
        safeAwait(testListener); // shouldn't throw
        assertTrue(cloneFuture.isDone());
    }

    @TestLogging(reason = "testing logging at INFO level", value = "org.elasticsearch.snapshots.SnapshotsService:INFO")
    public void testFullSnapshotUnassignedShards() {
        setupTestCluster(1, 0); // no data nodes, we want unassigned shards

        final var indices = IntStream.range(0, between(1, 4)).mapToObj(i -> "index-" + i).sorted().toList();
        final var repoName = "repo";

        var testListener = SubscribableListener

            // Create the repo and indices
            .<Void>newForked(stepListener -> {
                try (var listeners = new RefCountingListener(stepListener)) {
                    client().admin()
                        .cluster()
                        .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                        .setType(FsRepository.TYPE)
                        .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
                        .execute(listeners.acquire(createRepoResponse -> {}));

                    for (final var index : indices) {
                        deterministicTaskQueue.scheduleNow(
                            // wrapped in another scheduleNow() to randomize creation order
                            ActionRunnable.<CreateIndexResponse>wrap(
                                listeners.acquire(createIndexResponse -> {}),
                                l -> client().admin()
                                    .indices()
                                    .create(
                                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.NONE)
                                            .settings(defaultIndexSettings(1)),
                                        l
                                    )
                            )
                        );
                    }
                }
            })

            // Take the snapshot to check the reaction to having unassigned shards
            .<Void>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier())
                    .setWaitForCompletion(randomBoolean())
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                            fail("snapshot should not have started");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(
                                asInstanceOf(SnapshotException.class, e).getMessage(),
                                allOf(
                                    containsString("the following indices have unassigned primary shards"),
                                    containsString("unless [partial] is set to [true]"),
                                    containsString(indices.toString() /* NB sorted */),
                                    containsString(ReferenceDocs.UNASSIGNED_SHARDS.toString())
                                )
                            );
                            l.onResponse(null);
                        }
                    })
            );

        MockLog.assertThatLogger(() -> {
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue("executed all runnable tasks but test steps are still incomplete", testListener.isDone());
            safeAwait(testListener); // shouldn't throw
        },
            SnapshotsServiceUtils.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsServiceUtils.class.getCanonicalName(),
                Level.INFO,
                "*failed to create snapshot*the following indices have unassigned primary shards*"
            )
        );
    }

    @TestLogging(reason = "testing logging at INFO level", value = "org.elasticsearch.snapshots.SnapshotsService:INFO")
    public void testSnapshotNameAlreadyInUseExceptionLogging() {
        setupTestCluster(1, 1);

        final var repoName = "repo";
        final var snapshotName = "test-snapshot";

        final var testListener = createRepoAndIndex(repoName, "index", between(1, 2))
            // take snapshot once
            .<CreateSnapshotResponse>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setWaitForCompletion(true)
                    .execute(l)
            )
            // take snapshot again
            .<CreateSnapshotResponse>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setWaitForCompletion(randomBoolean())
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                            fail("snapshot should not have started");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(SnapshotNameAlreadyInUseException.class));
                            l.onResponse(null);
                        }
                    })
            )
            // attempt to clone snapshot
            .<AcknowledgedResponse>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName, snapshotName)
                    .setIndices("*")
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            fail("snapshot should not have started");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(SnapshotNameAlreadyInUseException.class));
                            l.onResponse(null);
                        }
                    })
            );

        final var expectedMessage = Strings.format("Invalid snapshot name [%s], snapshot with the same name already exists", snapshotName);
        MockLog.assertThatLogger(() -> {
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue("executed all runnable tasks but test steps are still incomplete", testListener.isDone());
            safeAwait(testListener); // shouldn't throw
        },
            SnapshotsServiceUtils.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsServiceUtils.class.getCanonicalName(),
                Level.INFO,
                Strings.format("*failed to create snapshot*%s", expectedMessage)
            ),
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsServiceUtils.class.getCanonicalName(),
                Level.INFO,
                Strings.format("*failed to clone snapshot*%s", expectedMessage)
            )
        );
    }

    @TestLogging(reason = "testing logging at INFO level", value = "org.elasticsearch.snapshots.SnapshotsService:INFO")
    public void testIndexNotFoundExceptionLogging() {
        setupTestCluster(1, 0); // no need for data nodes here

        final var repoName = "repo";
        final var indexName = "does-not-exist";

        final var testListener = SubscribableListener
            // create repo
            .<AcknowledgedResponse>newForked(
                l -> client().admin()
                    .cluster()
                    .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                    .setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
                    .execute(l)
            )
            // take snapshot of index that does not exist
            .<CreateSnapshotResponse>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier())
                    .setIndices(indexName)
                    .setWaitForCompletion(randomBoolean())
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                            fail("snapshot should not have started");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IndexNotFoundException.class));
                            l.onResponse(null);
                        }
                    })
            );

        MockLog.assertThatLogger(() -> {
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue("executed all runnable tasks but test steps are still incomplete", testListener.isDone());
            safeAwait(testListener); // shouldn't throw
        },
            SnapshotsServiceUtils.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsServiceUtils.class.getCanonicalName(),
                Level.INFO,
                Strings.format("failed to create snapshot: no such index [%s]", indexName)
            )
        );
    }

    @TestLogging(reason = "testing logging at INFO level", value = "org.elasticsearch.snapshots.SnapshotsService:INFO")
    public void testIllegalArgumentExceptionLogging() {
        setupTestCluster(1, 0); // no need for data nodes here

        final var repoName = "repo";

        final var testListener = SubscribableListener
            // create repo
            .<AcknowledgedResponse>newForked(
                l -> client().admin()
                    .cluster()
                    .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                    .setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
                    .execute(l)
            )
            // attempt to take snapshot with illegal config ('none' is allowed as a feature state iff it's the only one in the list)
            .<CreateSnapshotResponse>andThen(
                l -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier())
                    .setFeatureStates("none", "none")
                    .setWaitForCompletion(randomBoolean())
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                            fail("snapshot should not have started");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IllegalArgumentException.class));
                            l.onResponse(null);
                        }
                    })
            );

        MockLog.assertThatLogger(() -> {
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue("executed all runnable tasks but test steps are still incomplete", testListener.isDone());
            safeAwait(testListener); // shouldn't throw
        },
            SnapshotsServiceUtils.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsServiceUtils.class.getCanonicalName(),
                Level.INFO,
                Strings.format("*failed to create snapshot*other feature states were requested: [none, none]", "")
            )
        );
    }

    protected RepositoryData getRepositoryData(Repository repository) {
        final PlainActionFuture<RepositoryData> res = new PlainActionFuture<>();
        repository.getRepositoryData(deterministicTaskQueue::scheduleNow, res);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(res.isDone());
        return res.actionGet();
    }

    protected SubscribableListener<CreateIndexResponse> createRepoAndIndex(String repoName, String index, int shards) {
        final SubscribableListener<AcknowledgedResponse> createRepositoryListener = new SubscribableListener<>();

        client().admin()
            .cluster()
            .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(createRepositoryListener);

        final SubscribableListener<CreateIndexResponse> createIndexResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepositoryListener,
            acknowledgedResponse -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL).settings(defaultIndexSettings(shards)),
                    createIndexResponseStepListener
                )
        );

        return createIndexResponseStepListener;
    }

    protected <T> SubscribableListener<Void> restoreSnapshotAndWaitForGreen(
        SubscribableListener<T> listener,
        String repoName,
        String snapshotName,
        String indexName,
        int expectedNumShards
    ) {
        final SubscribableListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new SubscribableListener<>();
        continueOrDie(
            listener,
            ignored -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).waitForCompletion(true),
                    restoreSnapshotResponseListener
                )
        );

        return waitForGreenAfterRestore(restoreSnapshotResponseListener, indexName, expectedNumShards);
    }

    private SubscribableListener<Void> waitForGreenAfterRestore(
        SubscribableListener<RestoreSnapshotResponse> restoreListener,
        String indexName,
        int expectedNumShards
    ) {
        final SubscribableListener<ClusterHealthResponse> clusterHealthResponseListener = new SubscribableListener<>();
        continueOrDie(restoreListener, restoreSnapshotResponse -> {
            assertEquals(expectedNumShards, restoreSnapshotResponse.getRestoreInfo().totalShards());

            client().admin()
                .cluster()
                .prepareHealth(TimeValue.MINUS_ONE, indexName)
                .setWaitForGreenStatus()
                .execute(clusterHealthResponseListener);
        });

        final SubscribableListener<Void> greenHealthListener = new SubscribableListener<>();
        continueOrDie(clusterHealthResponseListener, clusterHealthResponse -> {
            assertThat(clusterHealthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            greenHealthListener.onResponse(null);
        });
        return greenHealthListener;
    }

    protected void clearDisruptionsAndAwaitSync() {
        testClusterNodes.clearNetworkDisruptions();
        stabilize();
    }

    private void disconnectOrRestartDataNode() {
        if (randomBoolean()) {
            disconnectRandomDataNode();
        } else {
            testClusterNodes.randomDataNode().ifPresent(SnapshotResiliencyTestHelper.TestClusterNode::restart);
        }
    }

    protected void disconnectOrRestartMasterNode() {
        testClusterNodes.randomMasterNode().ifPresent(masterNode -> {
            if (randomBoolean()) {
                testClusterNodes.disconnectNode(masterNode);
            } else {
                masterNode.restart();
            }
        });
    }

    private void disconnectRandomDataNode() {
        testClusterNodes.randomDataNode().ifPresent(n -> testClusterNodes.disconnectNode(n));
    }

    protected void startCluster() {
        final ClusterState initialClusterState = new ClusterState.Builder(ClusterName.DEFAULT).nodes(testClusterNodes.discoveryNodes())
            .build();
        testClusterNodes.nodes().values().forEach(testClusterNode -> testClusterNode.start(initialClusterState));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        final VotingConfiguration votingConfiguration = createVotingConfiguration();
        testClusterNodes.nodes()
            .values()
            .stream()
            .filter(n -> n.node().isMasterNode())
            .forEach(testClusterNode -> testClusterNode.coordinator().setInitialConfiguration(votingConfiguration));
        // Connect all nodes to each other
        testClusterNodes.nodes()
            .values()
            .forEach(
                node -> testClusterNodes.nodes()
                    .values()
                    .forEach(
                        n -> n.transportService()
                            .connectToNode(
                                node.node(),
                                ActionTestUtils.assertNoFailureListener(
                                    c -> logger.info("--> Connected [{}] to [{}]", n.node(), node.node())
                                )
                            )
                    )
            );
        stabilize();
    }

    protected VotingConfiguration createVotingConfiguration() {
        final VotingConfiguration votingConfiguration = new VotingConfiguration(
            testClusterNodes.nodes()
                .values()
                .stream()
                .map(n -> n.node())
                .filter(DiscoveryNode::isMasterNode)
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet())
        );
        return votingConfiguration;
    }

    private void stabilize() {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + AbstractCoordinatorTestCase.DEFAULT_STABILISATION_TIME;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime) {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }
        runUntil(() -> {
            final Collection<ClusterState> clusterStates = testClusterNodes.nodes()
                .values()
                .stream()
                .map(node -> node.clusterService().state())
                .toList();
            final Set<String> masterNodeIds = clusterStates.stream()
                .map(clusterState -> clusterState.nodes().getMasterNodeId())
                .collect(Collectors.toSet());
            final Set<Long> terms = clusterStates.stream().map(ClusterState::term).collect(Collectors.toSet());
            final List<Long> versions = clusterStates.stream().map(ClusterState::version).distinct().toList();
            return versions.size() == 1 && masterNodeIds.size() == 1 && masterNodeIds.contains(null) == false && terms.size() == 1;
        }, TimeUnit.MINUTES.toMillis(1L));
    }

    protected void runUntil(Supplier<Boolean> fulfilled, long timeout) {
        final long start = deterministicTaskQueue.getCurrentTimeMillis();
        while (timeout > deterministicTaskQueue.getCurrentTimeMillis() - start) {
            if (fulfilled.get()) {
                return;
            }
            deterministicTaskQueue.runAllRunnableTasks();
            deterministicTaskQueue.advanceTime();
        }
        fail("Condition wasn't fulfilled.");
    }

    protected void setupTestCluster(int masterNodes, int dataNodes) {
        setupTestCluster(masterNodes, dataNodes, ignored -> TransportService.NOOP_TRANSPORT_INTERCEPTOR);
    }

    protected void setupTestCluster(int masterNodes, int dataNodes, TransportInterceptorFactory transportInterceptorFactory) {
        testClusterNodes = new SnapshotResiliencyTestHelper.TestClusterNodes(
            masterNodes,
            dataNodes,
            tempDir,
            deterministicTaskQueue,
            transportInterceptorFactory,
            this::assertCriticalWarnings
        );
        startCluster();
    }

    private void scheduleSoon(Runnable runnable) {
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(0, 100L), runnable);
    }

    protected void scheduleNow(Runnable runnable) {
        deterministicTaskQueue.scheduleNow(runnable);
    }

    protected Settings defaultIndexSettings(int shards) {
        // TODO: randomize replica count settings once recovery operations aren't blocking anymore
        return indexSettings(shards, 0).build();
    }

    protected static <T> void continueOrDie(SubscribableListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.addListener(ActionTestUtils.assertNoFailureListener(onResponse));
    }

    public NodeClient client() {
        // Select from sorted list of nodes
        final List<SnapshotResiliencyTestHelper.TestClusterNode> nodes = testClusterNodes.nodes()
            .values()
            .stream()
            .filter(n -> testClusterNodes.disconnectedNodes().contains(n.node().getName()) == false)
            .sorted(Comparator.comparing(n -> n.node().getName()))
            .toList();
        if (nodes.isEmpty()) {
            throw new AssertionError("No nodes available");
        }
        return randomFrom(nodes).client();
    }

    private static <T> T safeResult(SubscribableListener<T> listener) {
        assertTrue("listener is not complete", listener.isDone());
        return safeAwait(listener);
    }
}
