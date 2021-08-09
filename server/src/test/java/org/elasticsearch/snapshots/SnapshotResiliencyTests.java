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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.plan.SourceOnlyRecoveryPlannerService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class SnapshotResiliencyTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    private TestClusterNodes testClusterNodes;

    private Path tempDir;

    @Before
    public void createServices() {
        tempDir = createTempDir();
        deterministicTaskQueue = new DeterministicTaskQueue();
    }

    @After
    public void verifyReposThenStopServices() {
        try {
            clearDisruptionsAndAwaitSync();

            final StepListener<CleanupRepositoryResponse> cleanupResponse = new StepListener<>();
            final StepListener<CreateSnapshotResponse> createSnapshotResponse = new StepListener<>();
            // Create another snapshot and then clean up the repository to verify that the repository works correctly no matter the
            // failures seen during the previous test.
            client().admin()
                .cluster()
                .prepareCreateSnapshot("repo", "last-snapshot")
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute(createSnapshotResponse);
            continueOrDie(createSnapshotResponse, r -> {
                final SnapshotInfo snapshotInfo = r.getSnapshotInfo();
                // Snapshot can be partial because some tests leave indices in a red state because data nodes were stopped
                assertThat(snapshotInfo.state(), either(is(SnapshotState.SUCCESS)).or(is(SnapshotState.PARTIAL)));
                assertThat(snapshotInfo.shardFailures(), iterableWithSize(snapshotInfo.failedShards()));
                assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards() - snapshotInfo.failedShards()));
                client().admin().cluster().cleanupRepository(new CleanupRepositoryRequest("repo"), cleanupResponse);
            });
            final AtomicBoolean cleanedUp = new AtomicBoolean(false);
            continueOrDie(cleanupResponse, r -> cleanedUp.set(true));

            runUntil(cleanedUp::get, TimeUnit.MINUTES.toMillis(1L));

            final PlainActionFuture<AssertionError> future = BlobStoreTestUtil.assertConsistencyAsync(
                (BlobStoreRepository) testClusterNodes.randomMasterNodeSafe().repositoriesService.repository("repo")
            );
            deterministicTaskQueue.runAllRunnableTasks();
            assertNull(future.actionGet(0));
        } finally {
            testClusterNodes.nodes.values().forEach(TestClusterNodes.TestClusterNode::stop);
        }
    }

    public void testSuccessfulSnapshotAndRestore() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(0, 100);

        final TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseListener = new StepListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final Runnable afterIndexing = () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseListener);
            if (documents == 0) {
                afterIndexing.run();
            } else {
                final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int i = 0; i < documents; ++i) {
                    bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
                }
                final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
                client().bulk(bulkRequest, bulkResponseStepListener);
                continueOrDie(bulkResponseStepListener, bulkResponse -> {
                    assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                    assertEquals(documents, bulkResponse.getItems().length);
                    afterIndexing.run();
                });
            }
        });

        final StepListener<AcknowledgedResponse> deleteIndexListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseListener,
            createSnapshotResponse -> client().admin().indices().delete(new DeleteIndexRequest(index), deleteIndexListener)
        );

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new StepListener<>();
        continueOrDie(
            deleteIndexListener,
            ignored -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapshotName).waitForCompletion(true),
                    restoreSnapshotResponseListener
                )
        );

        final StepListener<SearchResponse> searchResponseListener = new StepListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest(index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchResponseListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();
        continueOrDie(searchResponseListener, r -> {
            assertEquals(documents, Objects.requireNonNull(r.getHits().getTotalHits()).value);
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));
        assertNotNull(createSnapshotResponseListener.result());
        assertNotNull(restoreSnapshotResponseListener.result());
        assertTrue(documentCountVerified.get());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    private SnapshotInfo getSnapshotInfo(Repository repository, SnapshotId snapshotId) {
        final StepListener<SnapshotInfo> listener = new StepListener<>();
        repository.getSnapshotInfo(snapshotId, listener);
        deterministicTaskQueue.runAllRunnableTasks();
        return listener.result();
    }

    public void testSnapshotWithNodeDisconnects() {
        final int dataNodes = randomIntBetween(2, 10);
        final int masterNodes = randomFrom(1, 3, 5);
        setupTestCluster(masterNodes, dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final boolean partial = randomBoolean();
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                scheduleNow(this::disconnectRandomDataNode);
            }
            if (randomBoolean()) {
                scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
            }
            testClusterNodes.randomMasterNodeSafe().client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setPartial(partial)
                .execute(createSnapshotResponseStepListener);
        });

        final AtomicBoolean snapshotNeverStarted = new AtomicBoolean(false);

        createSnapshotResponseStepListener.whenComplete(createSnapshotResponse -> {
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
                final SnapshotException unwrapped = (SnapshotException) ExceptionsHelper.unwrap(e, SnapshotException.class);
                assertNotNull(unwrapped);
                assertThat(unwrapped.getMessage(), endsWith("Indices don't have primary shards [test]"));
                snapshotNeverStarted.set(true);
            } else {
                throw new AssertionError(e);
            }
        });

        runUntil(() -> testClusterNodes.randomMasterNode().map(master -> {
            if (snapshotNeverStarted.get()) {
                return true;
            }
            final SnapshotsInProgress snapshotsInProgress = master.clusterService.state().custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        SnapshotsInProgress finalSnapshotsInProgress = randomMaster.clusterService.state()
            .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = randomMaster.repositoriesService.repository(repoName);
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
        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> testClusterNodes.randomMasterNodeSafe().client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(waitForSnapshot)
                .execute(createSnapshotResponseStepListener)
        );

        final AtomicBoolean snapshotDeleteResponded = new AtomicBoolean(false);
        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(this::disconnectOrRestartMasterNode);
            testClusterNodes.randomDataNodeSafe().client.admin()
                .cluster()
                .prepareDeleteSnapshot(repoName, snapshotName)
                .execute(ActionListener.wrap(() -> snapshotDeleteResponded.set(true)));
        });

        runUntil(
            () -> testClusterNodes.randomMasterNode()
                .map(
                    master -> snapshotDeleteResponded.get()
                        && master.clusterService.state()
                            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
                            .getEntries()
                            .isEmpty()
                )
                .orElse(false),
            TimeUnit.MINUTES.toMillis(1L)
        );

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        SnapshotsInProgress finalSnapshotsInProgress = randomMaster.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = randomMaster.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(0));
    }

    public void testConcurrentSnapshotCreateAndDelete() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .execute(createSnapshotResponseStepListener)
        );

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        masterNode.clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty() == false) {
                    client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute(deleteSnapshotStepListener);
                    masterNode.clusterService.removeListener(this);
                }
            }
        });

        final StepListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            deleteSnapshotStepListener,
            acknowledgedResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener)
        );
        continueOrDie(
            createAnotherSnapshotResponseStepListener,
            createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        assertNotNull(createSnapshotResponseStepListener.result());
        assertNotNull(createAnotherSnapshotResponseStepListener.result());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final StepListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, "snapshot-2")
                .execute(createOtherSnapshotResponseStepListener)
        );

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareDeleteSnapshot(repoName, snapshotName)
                .execute(deleteSnapshotStepListener)
        );

        final StepListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(deleteSnapshotStepListener, deleted -> {
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener);
            continueOrDie(
                createAnotherSnapshotResponseStepListener,
                createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final int inProgressSnapshots = randomIntBetween(1, 5);
        final StepListener<Collection<CreateSnapshotResponse>> createOtherSnapshotResponseStepListener = new StepListener<>();
        final ActionListener<CreateSnapshotResponse> createSnapshotListener = new GroupedActionListener<>(
            createOtherSnapshotResponseStepListener,
            inProgressSnapshots
        );

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (int i = 0; i < inProgressSnapshots; i++) {
                client().admin().cluster().prepareCreateSnapshot(repoName, "other-" + i).execute(createSnapshotListener);
            }
        });

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .deleteSnapshot(new DeleteSnapshotRequest(repoName, "*"), deleteSnapshotStepListener)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final int documentsFirstSnapshot = randomIntBetween(0, 100);

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> indexNDocuments(
                documentsFirstSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, snapshotName)
                    .setWaitForCompletion(true)
                    .execute(createSnapshotResponseStepListener)
            )
        );

        final int documentsSecondSnapshot = randomIntBetween(0, 100);

        final StepListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new StepListener<>();

        final String secondSnapshotName = "snapshot-2";
        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> indexNDocuments(
                documentsSecondSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, secondSnapshotName)
                    .setWaitForCompletion(true)
                    .execute(createOtherSnapshotResponseStepListener)
            )
        );

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();
        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new StepListener<>();

        continueOrDie(createOtherSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(() -> client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute(deleteSnapshotStepListener));
            scheduleNow(
                () -> client().admin()
                    .cluster()
                    .restoreSnapshot(
                        new RestoreSnapshotRequest(repoName, secondSnapshotName).waitForCompletion(true)
                            .renamePattern("(.+)")
                            .renameReplacement("restored_$1"),
                        restoreSnapshotResponseListener
                    )
            );
        });

        final StepListener<SearchResponse> searchResponseListener = new StepListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest("restored_" + index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchResponseListener
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        assertEquals(
            documentsFirstSnapshot + documentsSecondSnapshot,
            Objects.requireNonNull(searchResponseListener.result().getHits().getTotalHits()).value
        );
        assertThat(deleteSnapshotStepListener.result().isAcknowledged(), is(true));
        assertThat(restoreSnapshotResponseListener.result().getRestoreInfo().failedShards(), is(0));

        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, contains(createOtherSnapshotResponseStepListener.result().getSnapshotInfo().snapshotId()));

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
        final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<Collection<CreateIndexResponse>> createIndicesListener = new StepListener<>();
        final int indices = randomIntBetween(5, 20);

        final SetOnce<Index> firstIndex = new SetOnce<>();
        continueOrDie(createRepoAndIndex(repoName, index, 1), createIndexResponse -> {
            firstIndex.set(masterNode.clusterService.state().metadata().index(index).getIndex());
            // create a few more indices to make it more likely that the subsequent index delete operation happens before snapshot
            // finalization
            final GroupedActionListener<CreateIndexResponse> listener = new GroupedActionListener<>(createIndicesListener, indices);
            for (int i = 0; i < indices; ++i) {
                client().admin().indices().create(new CreateIndexRequest("index-" + i), listener);
            }
        });

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final boolean partialSnapshot = randomBoolean();

        continueOrDie(
            createIndicesListener,
            createIndexResponses -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
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
                        client().admin().indices().create(new CreateIndexRequest(index), noopListener());
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

        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final Collection<StepListener<Boolean>> deleteSnapshotStepListeners = List.of(new StepListener<>(), new StepListener<>());

        final AtomicInteger successfulDeletes = new AtomicInteger(0);

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (StepListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
                client().admin()
                    .cluster()
                    .prepareDeleteSnapshot(repoName, snapshotName)
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

        for (StepListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
            continueOrDie(deleteListener, deleted -> {
                if (deleted) {
                    successfulDeletes.incrementAndGet();
                }
            });
        }

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotDeletionsInProgress deletionsInProgress = masterNode.clusterService.state().custom(SnapshotDeletionsInProgress.TYPE);
        assertFalse(deletionsInProgress.hasDeletionsInProgress());
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        final TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        final AdminClient masterAdminClient = masterNode.client.admin();

        final StepListener<ClusterStateResponse> clusterStateResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin().cluster().state(new ClusterStateRequest(), clusterStateResponseStepListener)
        );

        continueOrDie(clusterStateResponseStepListener, clusterStateResponse -> {
            final ShardRouting shardToRelocate = clusterStateResponse.getState().routingTable().allShards(index).get(0);
            final TestClusterNodes.TestClusterNode currentPrimaryNode = testClusterNodes.nodeById(shardToRelocate.currentNodeId());
            final TestClusterNodes.TestClusterNode otherNode = testClusterNodes.randomDataNodeSafe(currentPrimaryNode.node.getName());
            scheduleNow(() -> testClusterNodes.stopNode(currentPrimaryNode));
            scheduleNow(new Runnable() {
                @Override
                public void run() {
                    final StepListener<ClusterStateResponse> updatedClusterStateResponseStepListener = new StepListener<>();
                    masterAdminClient.cluster().state(new ClusterStateRequest(), updatedClusterStateResponseStepListener);
                    continueOrDie(updatedClusterStateResponseStepListener, updatedClusterState -> {
                        final ShardRouting shardRouting = updatedClusterState.getState()
                            .routingTable()
                            .shardRoutingTable(shardToRelocate.shardId())
                            .primaryShard();
                        if (shardRouting.unassigned() && shardRouting.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_LEFT) {
                            if (masterNodeCount > 1) {
                                scheduleNow(() -> testClusterNodes.stopNode(masterNode));
                            }
                            testClusterNodes.randomDataNodeSafe().client.admin()
                                .cluster()
                                .prepareCreateSnapshot(repoName, snapshotName)
                                .execute(ActionListener.wrap(() -> {
                                    createdSnapshot.set(true);
                                    testClusterNodes.randomDataNodeSafe().client.admin()
                                        .cluster()
                                        .deleteSnapshot(new DeleteSnapshotRequest(repoName, snapshotName), noopListener());
                                }));
                            scheduleNow(
                                () -> testClusterNodes.randomMasterNodeSafe().client.admin()
                                    .cluster()
                                    .reroute(
                                        new ClusterRerouteRequest().add(
                                            new AllocateEmptyPrimaryAllocationCommand(
                                                index,
                                                shardRouting.shardId().id(),
                                                otherNode.node.getName(),
                                                true
                                            )
                                        ),
                                        noopListener()
                                    )
                            );
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
            return master.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        assertThat(
            testClusterNodes.randomDataNodeSafe().clusterService.state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries(),
            empty()
        );
        final Repository repository = testClusterNodes.randomMasterNodeSafe().repositoriesService.repository(repoName);
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
        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

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
                                .prepareCreateSnapshot(repoName, snapshotName)
                                .setWaitForCompletion(true)
                                .execute(createSnapshotResponseStepListener);
                        }
                    })
                );
            }
        });

        final String restoredIndex = "restored";

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapshotName).renamePattern(index)
                        .renameReplacement(restoredIndex)
                        .waitForCompletion(true),
                    restoreSnapshotResponseStepListener
                )
        );

        final StepListener<SearchResponse> searchResponseStepListener = new StepListener<>();

        continueOrDie(restoreSnapshotResponseStepListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest(restoredIndex).source(new SearchSourceBuilder().size(documents).trackTotalHits(true)),
                searchResponseStepListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();

        continueOrDie(searchResponseStepListener, r -> {
            final long hitCount = r.getHits().getTotalHits().value;
            assertThat(
                "Documents were restored but the restored index mapping was older than some documents and misses some of their fields",
                (int) hitCount,
                lessThanOrEqualTo(
                    ((Map<?, ?>) masterNode.clusterService.state()
                        .metadata()
                        .index(restoredIndex)
                        .mapping()
                        .sourceAsMap()
                        .get("properties")).size()
                )
            );
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));

        assertNotNull(createSnapshotResponseStepListener.result());
        assertNotNull(restoreSnapshotResponseStepListener.result());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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
        final List<String> snapshotNames = IntStream.range(1, randomIntBetween(2, 4))
            .mapToObj(i -> "snapshot-" + i)
            .collect(Collectors.toList());
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(1, 100);

        final TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<Collection<CreateSnapshotResponse>> allSnapshotsListener = new StepListener<>();
        final ActionListener<CreateSnapshotResponse> snapshotListener = new GroupedActionListener<>(
            allSnapshotsListener,
            snapshotNames.size()
        );
        final AtomicBoolean doneIndexing = new AtomicBoolean(false);
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (String snapshotName : snapshotNames) {
                scheduleNow(
                    () -> client().admin()
                        .cluster()
                        .prepareCreateSnapshot(repoName, snapshotName)
                        .setWaitForCompletion(true)
                        .execute(snapshotListener)
                );
            }
            final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < documents; ++i) {
                bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
            }
            final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
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
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

    private RepositoryData getRepositoryData(Repository repository) {
        final PlainActionFuture<RepositoryData> res = PlainActionFuture.newFuture();
        repository.getRepositoryData(res);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(res.isDone());
        return res.actionGet();
    }

    private StepListener<CreateIndexResponse> createRepoAndIndex(String repoName, String index, int shards) {
        final StepListener<AcknowledgedResponse> createRepositoryListener = new StepListener<>();

        client().admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(createRepositoryListener);

        final StepListener<CreateIndexResponse> createIndexResponseStepListener = new StepListener<>();

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

    private void clearDisruptionsAndAwaitSync() {
        testClusterNodes.clearNetworkDisruptions();
        stabilize();
    }

    private void disconnectOrRestartDataNode() {
        if (randomBoolean()) {
            disconnectRandomDataNode();
        } else {
            testClusterNodes.randomDataNode().ifPresent(TestClusterNodes.TestClusterNode::restart);
        }
    }

    private void disconnectOrRestartMasterNode() {
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

    private void startCluster() {
        final ClusterState initialClusterState = new ClusterState.Builder(ClusterName.DEFAULT).nodes(testClusterNodes.discoveryNodes())
            .build();
        testClusterNodes.nodes.values().forEach(testClusterNode -> testClusterNode.start(initialClusterState));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        final VotingConfiguration votingConfiguration = new VotingConfiguration(
            testClusterNodes.nodes.values()
                .stream()
                .map(n -> n.node)
                .filter(DiscoveryNode::isMasterNode)
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet())
        );
        testClusterNodes.nodes.values()
            .stream()
            .filter(n -> n.node.isMasterNode())
            .forEach(testClusterNode -> testClusterNode.coordinator.setInitialConfiguration(votingConfiguration));
        // Connect all nodes to each other
        testClusterNodes.nodes.values()
            .forEach(
                node -> testClusterNodes.nodes.values()
                    .forEach(
                        n -> n.transportService.connectToNode(
                            node.node,
                            null,
                            ActionTestUtils.assertNoFailureListener(c -> logger.info("--> Connected [{}] to [{}]", n.node, node.node))
                        )
                    )
            );
        stabilize();
    }

    private void stabilize() {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + AbstractCoordinatorTestCase.DEFAULT_STABILISATION_TIME;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime) {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }
        runUntil(() -> {
            final Collection<ClusterState> clusterStates = testClusterNodes.nodes.values()
                .stream()
                .map(node -> node.clusterService.state())
                .collect(Collectors.toList());
            final Set<String> masterNodeIds = clusterStates.stream()
                .map(clusterState -> clusterState.nodes().getMasterNodeId())
                .collect(Collectors.toSet());
            final Set<Long> terms = clusterStates.stream().map(ClusterState::term).collect(Collectors.toSet());
            final List<Long> versions = clusterStates.stream().map(ClusterState::version).distinct().collect(Collectors.toList());
            return versions.size() == 1 && masterNodeIds.size() == 1 && masterNodeIds.contains(null) == false && terms.size() == 1;
        }, TimeUnit.MINUTES.toMillis(1L));
    }

    private void runUntil(Supplier<Boolean> fulfilled, long timeout) {
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

    private void setupTestCluster(int masterNodes, int dataNodes) {
        testClusterNodes = new TestClusterNodes(masterNodes, dataNodes);
        startCluster();
    }

    private void scheduleSoon(Runnable runnable) {
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(0, 100L), runnable);
    }

    private void scheduleNow(Runnable runnable) {
        deterministicTaskQueue.scheduleNow(runnable);
    }

    private static Settings defaultIndexSettings(int shards) {
        // TODO: randomize replica count settings once recovery operations aren't blocking anymore
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shards)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
    }

    private static <T> void continueOrDie(StepListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.whenComplete(onResponse, e -> { throw new AssertionError(e); });
    }

    private static <T> ActionListener<T> noopListener() {
        return ActionListener.wrap(() -> {});
    }

    public NodeClient client() {
        // Select from sorted list of nodes
        final List<TestClusterNodes.TestClusterNode> nodes = testClusterNodes.nodes.values()
            .stream()
            .filter(n -> testClusterNodes.disconnectedNodes.contains(n.node.getName()) == false)
            .sorted(Comparator.comparing(n -> n.node.getName()))
            .collect(Collectors.toList());
        if (nodes.isEmpty()) {
            throw new AssertionError("No nodes available");
        }
        return randomFrom(nodes).client;
    }

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private Environment createEnvironment(String nodeName) {
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), nodeName)
                .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
                .putList(
                    ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                    ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY)
                )
                .put(MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING.getKey(), 1000) // o.w. some tests might block
                .build()
        );
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        // Remove and add back local node to update ephemeral id on restarts
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).remove(node.getId()).add(node).localNodeId(node.getId()))
            .build();
    }

    private final class TestClusterNodes {

        // LinkedHashMap so we have deterministic ordering when iterating over the map in tests
        private final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();

        /**
         * Node names that are disconnected from all other nodes.
         */
        private final Set<String> disconnectedNodes = new HashSet<>();

        TestClusterNodes(int masterNodes, int dataNodes) {
            for (int i = 0; i < masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return newMasterNode(nodeName);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
            for (int i = 0; i < dataNodes; ++i) {
                nodes.computeIfAbsent("data-node" + i, nodeName -> {
                    try {
                        return newDataNode(nodeName);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
        }

        public TestClusterNode nodeById(final String nodeId) {
            return nodes.values()
                .stream()
                .filter(n -> n.node.getId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find node by id [" + nodeId + ']'));
        }

        private TestClusterNode newMasterNode(String nodeName) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.MASTER_ROLE);
        }

        private TestClusterNode newDataNode(String nodeName) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.DATA_ROLE);
        }

        private TestClusterNode newNode(String nodeName, DiscoveryNodeRole role) throws IOException {
            return new TestClusterNode(
                new DiscoveryNode(
                    nodeName,
                    randomAlphaOfLength(10),
                    buildNewFakeTransportAddress(),
                    emptyMap(),
                    Collections.singleton(role),
                    Version.CURRENT
                )
            );
        }

        public TestClusterNode randomMasterNodeSafe() {
            return randomMasterNode().orElseThrow(() -> new AssertionError("Expected to find at least one connected master node"));
        }

        public Optional<TestClusterNode> randomMasterNode() {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> masterNodes = testClusterNodes.nodes.values()
                .stream()
                .filter(n -> n.node.isMasterNode())
                .filter(n -> disconnectedNodes.contains(n.node.getName()) == false)
                .sorted(Comparator.comparing(n -> n.node.getName()))
                .collect(Collectors.toList());
            return masterNodes.isEmpty() ? Optional.empty() : Optional.of(randomFrom(masterNodes));
        }

        public void stopNode(TestClusterNode node) {
            node.stop();
            nodes.remove(node.node.getName());
        }

        public TestClusterNode randomDataNodeSafe(String... excludedNames) {
            return randomDataNode(excludedNames).orElseThrow(() -> new AssertionError("Could not find another data node."));
        }

        public Optional<TestClusterNode> randomDataNode(String... excludedNames) {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> dataNodes = testClusterNodes.nodes.values()
                .stream()
                .filter(n -> n.node.canContainData())
                .filter(n -> {
                    for (final String nodeName : excludedNames) {
                        if (n.node.getName().equals(nodeName)) {
                            return false;
                        }
                    }
                    return true;
                })
                .sorted(Comparator.comparing(n -> n.node.getName()))
                .collect(Collectors.toList());
            return dataNodes.isEmpty() ? Optional.empty() : Optional.ofNullable(randomFrom(dataNodes));
        }

        public void disconnectNode(TestClusterNode node) {
            if (disconnectedNodes.contains(node.node.getName())) {
                return;
            }
            testClusterNodes.nodes.values().forEach(n -> n.transportService.getConnectionManager().disconnectFromNode(node.node));
            disconnectedNodes.add(node.node.getName());
        }

        public void clearNetworkDisruptions() {
            final Set<String> disconnectedNodes = new HashSet<>(this.disconnectedNodes);
            this.disconnectedNodes.clear();
            disconnectedNodes.forEach(nodeName -> {
                if (testClusterNodes.nodes.containsKey(nodeName)) {
                    final DiscoveryNode node = testClusterNodes.nodes.get(nodeName).node;
                    testClusterNodes.nodes.values()
                        .forEach(
                            n -> n.transportService.openConnection(
                                node,
                                null,
                                ActionTestUtils.assertNoFailureListener(c -> logger.debug("--> Connected [{}] to [{}]", n.node, node))
                            )
                        );
                }
            });
        }

        /**
         * Builds a {@link DiscoveryNodes} instance that holds the nodes in this test cluster.
         * @return DiscoveryNodes
         */
        public DiscoveryNodes discoveryNodes() {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            nodes.values().forEach(node -> builder.add(node.node));
            return builder.build();
        }

        /**
         * Returns the {@link TestClusterNode} for the master node in the given {@link ClusterState}.
         * @param state ClusterState
         * @return Master Node
         */
        public TestClusterNode currentMaster(ClusterState state) {
            TestClusterNode master = nodes.get(state.nodes().getMasterNode().getName());
            assertNotNull(master);
            assertTrue(master.node.isMasterNode());
            return master;
        }

        private final class TestClusterNode {

            private final Logger logger = LogManager.getLogger(TestClusterNode.class);

            private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Stream.concat(ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream())
                    .collect(Collectors.toList())
            );

            private final TransportService transportService;

            private final ClusterService clusterService;

            private final RecoverySettings recoverySettings;

            private final PeerRecoverySourceService peerRecoverySourceService;

            private final NodeConnectionsService nodeConnectionsService;

            private final RepositoriesService repositoriesService;

            private final SnapshotsService snapshotsService;

            private final SnapshotShardsService snapshotShardsService;

            private final IndicesService indicesService;

            private final IndicesClusterStateService indicesClusterStateService;

            private final DiscoveryNode node;

            private final MasterService masterService;

            private final AllocationService allocationService;

            private final RerouteService rerouteService;

            private final NodeClient client;

            private final NodeEnvironment nodeEnv;

            private final DisruptableMockTransport mockTransport;

            private final ThreadPool threadPool;

            private final BigArrays bigArrays;

            private Coordinator coordinator;

            TestClusterNode(DiscoveryNode node) throws IOException {
                this.node = node;
                final Environment environment = createEnvironment(node.getName());
                threadPool = deterministicTaskQueue.getThreadPool(runnable -> DeterministicTaskQueue.onNodeLog(node, runnable));
                masterService = new FakeThreadPoolMasterService(node.getName(), "test", threadPool, deterministicTaskQueue::scheduleNow);
                final Settings settings = environment.settings();
                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterService = new ClusterService(
                    settings,
                    clusterSettings,
                    masterService,
                    new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                        @Override
                        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                            return new MockSinglePrioritizingExecutor(node.getName(), deterministicTaskQueue, threadPool);
                        }

                        @Override
                        protected void connectToNodesAndWait(ClusterState newClusterState) {
                            // don't do anything, and don't block
                        }
                    }
                );
                recoverySettings = new RecoverySettings(settings, clusterSettings);
                mockTransport = new DisruptableMockTransport(node, logger, deterministicTaskQueue) {
                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                        if (node.equals(destination)) {
                            return ConnectionStatus.CONNECTED;
                        }
                        // Check if both nodes are still part of the cluster
                        if (nodes.containsKey(node.getName()) == false || nodes.containsKey(destination.getName()) == false) {
                            return ConnectionStatus.DISCONNECTED;
                        }
                        return disconnectedNodes.contains(node.getName()) || disconnectedNodes.contains(destination.getName())
                            ? ConnectionStatus.DISCONNECTED
                            : ConnectionStatus.CONNECTED;
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                        return nodes.values()
                            .stream()
                            .map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                            .findAny();
                    }

                    @Override
                    protected void execute(Runnable runnable) {
                        scheduleNow(DeterministicTaskQueue.onNodeLog(getLocalNode(), runnable));
                    }

                    @Override
                    protected NamedWriteableRegistry writeableRegistry() {
                        return namedWriteableRegistry;
                    }
                };
                transportService = mockTransport.createTransportService(
                    settings,
                    threadPool,
                    TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                    a -> node,
                    null,
                    emptySet()
                );
                final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
                bigArrays = new BigArrays(new PageCacheRecycler(settings), null, "test");
                repositoriesService = new RepositoriesService(
                    settings,
                    clusterService,
                    transportService,
                    Collections.singletonMap(
                        FsRepository.TYPE,
                        metadata -> new FsRepository(
                            metadata,
                            environment,
                            xContentRegistry(),
                            clusterService,
                            bigArrays,
                            recoverySettings
                        ) {
                            @Override
                            protected void assertSnapshotOrGenericThread() {
                                // eliminate thread name check as we create repo in the test thread
                            }
                        }
                    ),
                    emptyMap(),
                    threadPool
                );
                final ActionFilters actionFilters = new ActionFilters(emptySet());
                snapshotsService = new SnapshotsService(
                    settings,
                    clusterService,
                    indexNameExpressionResolver,
                    repositoriesService,
                    transportService,
                    actionFilters,
                    Collections.emptyMap()
                );
                nodeEnv = new NodeEnvironment(settings, environment);
                final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
                final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap());
                client = new NodeClient(settings, threadPool);
                final SetOnce<RerouteService> rerouteServiceSetOnce = new SetOnce<>();
                final SnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                    settings,
                    clusterService,
                    () -> repositoriesService,
                    rerouteServiceSetOnce::get
                );
                allocationService = ESAllocationTestCase.createAllocationService(settings, snapshotsInfoService);
                rerouteService = new BatchedRerouteService(clusterService, allocationService::reroute);
                rerouteServiceSetOnce.set(rerouteService);
                final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
                    settings,
                    IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
                );
                final MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
                indicesService = new IndicesService(
                    settings,
                    mock(PluginsService.class),
                    nodeEnv,
                    namedXContentRegistry,
                    new AnalysisRegistry(
                        environment,
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap()
                    ),
                    indexNameExpressionResolver,
                    mapperRegistry,
                    namedWriteableRegistry,
                    threadPool,
                    indexScopedSettings,
                    new NoneCircuitBreakerService(),
                    bigArrays,
                    scriptService,
                    clusterService,
                    client,
                    new MetaStateService(nodeEnv, namedXContentRegistry),
                    Collections.emptyList(),
                    emptyMap(),
                    null,
                    emptyMap(),
                    List.of(),
                    emptyMap(),
                    null
                );
                final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
                snapshotShardsService = new SnapshotShardsService(
                    settings,
                    clusterService,
                    repositoriesService,
                    transportService,
                    indicesService
                );
                final ShardStateAction shardStateAction = new ShardStateAction(
                    clusterService,
                    transportService,
                    allocationService,
                    rerouteService,
                    threadPool
                );
                nodeConnectionsService = new NodeConnectionsService(clusterService.getSettings(), threadPool, transportService);
                Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions =
                    new HashMap<>();
                actions.put(
                    GlobalCheckpointSyncAction.TYPE,
                    new GlobalCheckpointSyncAction(
                        settings,
                        transportService,
                        clusterService,
                        indicesService,
                        threadPool,
                        shardStateAction,
                        actionFilters
                    )
                );
                final MetadataMappingService metadataMappingService = new MetadataMappingService(clusterService, indicesService);

                peerRecoverySourceService = new PeerRecoverySourceService(
                    transportService,
                    indicesService,
                    recoverySettings,
                    SourceOnlyRecoveryPlannerService.INSTANCE
                );

                indicesClusterStateService = new IndicesClusterStateService(
                    settings,
                    indicesService,
                    clusterService,
                    threadPool,
                    new PeerRecoveryTargetService(threadPool, transportService, recoverySettings, clusterService),
                    shardStateAction,
                    repositoriesService,
                    mock(SearchService.class),
                    peerRecoverySourceService,
                    snapshotShardsService,
                    new PrimaryReplicaSyncer(
                        transportService,
                        new TransportResyncReplicationAction(
                            settings,
                            transportService,
                            clusterService,
                            indicesService,
                            threadPool,
                            shardStateAction,
                            actionFilters,
                            new IndexingPressure(settings),
                            EmptySystemIndices.INSTANCE
                        )
                    ),
                    RetentionLeaseSyncer.EMPTY,
                    client
                );
                final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);
                final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                    settings,
                    clusterService,
                    indicesService,
                    allocationService,
                    new AliasValidator(),
                    shardLimitValidator,
                    environment,
                    indexScopedSettings,
                    threadPool,
                    namedXContentRegistry,
                    EmptySystemIndices.INSTANCE,
                    false
                );
                actions.put(
                    CreateIndexAction.INSTANCE,
                    new TransportCreateIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataCreateIndexService,
                        actionFilters,
                        indexNameExpressionResolver,
                        EmptySystemIndices.INSTANCE
                    )
                );
                final MappingUpdatedAction mappingUpdatedAction = new MappingUpdatedAction(settings, clusterSettings);
                final IndexingPressure indexingMemoryLimits = new IndexingPressure(settings);
                mappingUpdatedAction.setClient(client);
                actions.put(
                    BulkAction.INSTANCE,
                    new TransportBulkAction(
                        threadPool,
                        transportService,
                        clusterService,
                        new IngestService(
                            clusterService,
                            threadPool,
                            environment,
                            scriptService,
                            new AnalysisModule(environment, Collections.emptyList()).getAnalysisRegistry(),
                            Collections.emptyList(),
                            client
                        ),
                        client,
                        actionFilters,
                        indexNameExpressionResolver,
                        new IndexingPressure(settings),
                        EmptySystemIndices.INSTANCE,
                        indicesService
                    )
                );
                final TransportShardBulkAction transportShardBulkAction = new TransportShardBulkAction(
                    settings,
                    transportService,
                    clusterService,
                    indicesService,
                    threadPool,
                    shardStateAction,
                    mappingUpdatedAction,
                    new UpdateHelper(scriptService),
                    actionFilters,
                    indexingMemoryLimits,
                    EmptySystemIndices.INSTANCE
                );
                actions.put(TransportShardBulkAction.TYPE, transportShardBulkAction);
                final RestoreService restoreService = new RestoreService(
                    clusterService,
                    repositoriesService,
                    allocationService,
                    metadataCreateIndexService,
                    new MetadataDeleteIndexService(settings, clusterService, allocationService),
                    new IndexMetadataVerifier(settings, namedXContentRegistry, mapperRegistry, indexScopedSettings, ScriptCompiler.NONE),
                    shardLimitValidator,
                    EmptySystemIndices.INSTANCE
                );
                actions.put(
                    PutMappingAction.INSTANCE,
                    new TransportPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new RequestValidators<>(Collections.emptyList()),
                        EmptySystemIndices.INSTANCE
                    )
                );
                actions.put(
                    AutoPutMappingAction.INSTANCE,
                    new TransportAutoPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver,
                        EmptySystemIndices.INSTANCE
                    )
                );
                final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
                final SearchTransportService searchTransportService = new SearchTransportService(
                    transportService,
                    client,
                    SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
                );
                final SearchService searchService = new SearchService(
                    clusterService,
                    indicesService,
                    threadPool,
                    scriptService,
                    bigArrays,
                    new FetchPhase(Collections.emptyList()),
                    responseCollectorService,
                    new NoneCircuitBreakerService(),
                    EmptySystemIndices.INSTANCE.getExecutorSelector()
                );
                SearchPhaseController searchPhaseController = new SearchPhaseController(
                    writableRegistry(),
                    searchService::aggReduceContextBuilder
                );
                actions.put(
                    SearchAction.INSTANCE,
                    new TransportSearchAction(
                        threadPool,
                        new NoneCircuitBreakerService(),
                        transportService,
                        searchService,
                        searchTransportService,
                        searchPhaseController,
                        clusterService,
                        actionFilters,
                        indexNameExpressionResolver,
                        namedWriteableRegistry,
                        EmptySystemIndices.INSTANCE.getExecutorSelector()
                    )
                );
                actions.put(
                    RestoreSnapshotAction.INSTANCE,
                    new TransportRestoreSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        restoreService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    DeleteIndexAction.INSTANCE,
                    new TransportDeleteIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        new MetadataDeleteIndexService(settings, clusterService, allocationService),
                        actionFilters,
                        indexNameExpressionResolver,
                        new DestructiveOperations(settings, clusterSettings)
                    )
                );
                actions.put(
                    PutRepositoryAction.INSTANCE,
                    new TransportPutRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    CleanupRepositoryAction.INSTANCE,
                    new TransportCleanupRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        snapshotsService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    CreateSnapshotAction.INSTANCE,
                    new TransportCreateSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    ClusterRerouteAction.INSTANCE,
                    new TransportClusterRerouteAction(
                        transportService,
                        clusterService,
                        threadPool,
                        allocationService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    ClusterStateAction.INSTANCE,
                    new TransportClusterStateAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    IndicesShardStoresAction.INSTANCE,
                    new TransportIndicesShardStoresAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        client
                    )
                );
                actions.put(
                    TransportNodesListGatewayStartedShards.TYPE,
                    new TransportNodesListGatewayStartedShards(
                        settings,
                        threadPool,
                        clusterService,
                        transportService,
                        actionFilters,
                        nodeEnv,
                        indicesService,
                        namedXContentRegistry
                    )
                );
                actions.put(
                    DeleteSnapshotAction.INSTANCE,
                    new TransportDeleteSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                client.initialize(
                    actions,
                    transportService.getTaskManager(),
                    () -> clusterService.localNode().getId(),
                    transportService.getLocalNodeConnection(),
                    transportService.getRemoteClusterService(),
                    new NamedWriteableRegistry(List.of())
                );
            }

            public void restart() {
                testClusterNodes.disconnectNode(this);
                final ClusterState oldState = this.clusterService.state();
                stop();
                nodes.remove(node.getName());
                scheduleSoon(() -> {
                    try {
                        final TestClusterNode restartedNode = new TestClusterNode(
                            new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), emptyMap(), node.getRoles(), Version.CURRENT)
                        );
                        nodes.put(node.getName(), restartedNode);
                        restartedNode.start(oldState);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }

            public void stop() {
                testClusterNodes.disconnectNode(this);
                indicesService.close();
                clusterService.close();
                nodeConnectionsService.stop();
                indicesClusterStateService.close();
                peerRecoverySourceService.stop();
                if (coordinator != null) {
                    coordinator.close();
                }
                nodeEnv.close();
            }

            public void start(ClusterState initialState) {
                transportService.start();
                transportService.acceptIncomingRequests();
                snapshotsService.start();
                snapshotShardsService.start();
                repositoriesService.start();
                final CoordinationState.PersistedState persistedState = new InMemoryPersistedState(
                    initialState.term(),
                    stateForNode(initialState, node)
                );
                coordinator = new Coordinator(
                    node.getName(),
                    clusterService.getSettings(),
                    clusterService.getClusterSettings(),
                    transportService,
                    namedWriteableRegistry,
                    allocationService,
                    masterService,
                    () -> persistedState,
                    hostsResolver -> nodes.values()
                        .stream()
                        .filter(n -> n.node.isMasterNode())
                        .map(n -> n.node.getAddress())
                        .collect(Collectors.toList()),
                    clusterService.getClusterApplierService(),
                    Collections.emptyList(),
                    random(),
                    rerouteService,
                    ElectionStrategy.DEFAULT_INSTANCE,
                    () -> new StatusInfo(HEALTHY, "healthy-info")
                );
                masterService.setClusterStatePublisher(coordinator);
                coordinator.start();
                clusterService.getClusterApplierService().setNodeConnectionsService(nodeConnectionsService);
                nodeConnectionsService.start();
                clusterService.start();
                indicesService.start();
                indicesClusterStateService.start();
                coordinator.startInitialJoin();
                peerRecoverySourceService.start();
            }
        }
    }
}
