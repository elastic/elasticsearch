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
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.node.NodeClient;
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
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
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
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesServiceBuilder;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.SnapshotFilesProvider;
import org.elasticsearch.indices.recovery.plan.PeerOnlyRecoveryPlannerService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.repositories.VerifyNodeRepositoryCoordinationAction;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.DisruptableMockTransport;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
                (BlobStoreRepository) testClusterNodes.randomMasterNodeSafe().repositoriesService.repository("repo")
            );
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(future.isDone());
            final var result = future.result();
            if (result != null) {
                fail(result);
            }
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

        final SubscribableListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new SubscribableListener<>();
        continueOrDie(
            deleteIndexListener,
            ignored -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).waitForCompletion(true),
                    restoreSnapshotResponseListener
                )
        );

        final SubscribableListener<SearchResponse> searchResponseListener = new SubscribableListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
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
        assertNotNull(safeResult(restoreSnapshotResponseListener));
        assertTrue(documentCountVerified.get());
        assertTrue(SnapshotsInProgress.get(masterNode.clusterService.state()).isEmpty());
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
            testClusterNodes.randomMasterNodeSafe().client.admin()
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
            final SnapshotsInProgress snapshotsInProgress = master.clusterService.state().custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        SnapshotsInProgress finalSnapshotsInProgress = SnapshotsInProgress.get(randomMaster.clusterService.state());
        assertTrue(finalSnapshotsInProgress.isEmpty());
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
        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> testClusterNodes.randomMasterNodeSafe().client.admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(waitForSnapshot)
                .execute(createSnapshotResponseStepListener)
        );

        final AtomicBoolean snapshotDeleteResponded = new AtomicBoolean(false);
        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(this::disconnectOrRestartMasterNode);
            testClusterNodes.randomDataNodeSafe().client.admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .execute(ActionListener.running(() -> snapshotDeleteResponded.set(true)));
        });

        runUntil(
            () -> testClusterNodes.randomMasterNode()
                .map(
                    master -> snapshotDeleteResponded.get()
                        && SnapshotDeletionsInProgress.get(master.clusterService.state()).getEntries().isEmpty()
                )
                .orElse(false),
            TimeUnit.MINUTES.toMillis(1L)
        );

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        assertTrue(SnapshotsInProgress.get(randomMaster.clusterService.state()).isEmpty());
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

        final SubscribableListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .execute(createSnapshotResponseStepListener)
        );

        final SubscribableListener<AcknowledgedResponse> deleteSnapshotStepListener = new SubscribableListener<>();

        masterNode.clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (SnapshotsInProgress.get(event.state()).isEmpty() == false) {
                    client().admin()
                        .cluster()
                        .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                        .execute(deleteSnapshotStepListener);
                    masterNode.clusterService.removeListener(this);
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
        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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

        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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

        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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

        final SubscribableListener<SearchResponse> searchResponseListener = new SubscribableListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
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

        final Repository repository = masterNode.repositoriesService.repository(repoName);
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

        TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final SubscribableListener<Collection<CreateIndexResponse>> createIndicesListener = new SubscribableListener<>();
        final int indices = randomIntBetween(5, 20);

        final SetOnce<Index> firstIndex = new SetOnce<>();
        continueOrDie(createRepoAndIndex(repoName, index, 1), createIndexResponse -> {
            firstIndex.set(masterNode.clusterService.state().metadata().getProject().index(index).getIndex());
            // create a few more indices to make it more likely that the subsequent index delete operation happens before snapshot
            // finalization
            final GroupedActionListener<CreateIndexResponse> listener = new GroupedActionListener<>(indices, createIndicesListener);
            for (int i = 0; i < indices; ++i) {
                client().admin().indices().create(new CreateIndexRequest("index-" + i), listener);
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
                        client().admin().indices().create(new CreateIndexRequest(index), ActionListener.noop());
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

        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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

        assertFalse(SnapshotDeletionsInProgress.get(masterNode.clusterService.state()).hasDeletionsInProgress());
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

        final SubscribableListener<ClusterStateResponse> clusterStateResponseStepListener = new SubscribableListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT), clusterStateResponseStepListener)
        );

        continueOrDie(clusterStateResponseStepListener, clusterStateResponse -> {
            final ShardRouting shardToRelocate = clusterStateResponse.getState().routingTable().allShards(index).get(0);
            final TestClusterNodes.TestClusterNode currentPrimaryNode = testClusterNodes.nodeById(shardToRelocate.currentNodeId());
            final TestClusterNodes.TestClusterNode otherNode = testClusterNodes.randomDataNodeSafe(currentPrimaryNode.node.getName());
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
                            testClusterNodes.randomDataNodeSafe().client.admin()
                                .cluster()
                                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                                .execute(ActionListener.running(() -> {
                                    createdSnapshot.set(true);
                                    testClusterNodes.randomDataNodeSafe().client.admin()
                                        .cluster()
                                        .deleteSnapshot(
                                            new DeleteSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName),
                                            ActionListener.noop()
                                        );
                                }));
                            scheduleNow(
                                () -> testClusterNodes.randomMasterNodeSafe().client.execute(
                                    TransportClusterRerouteAction.TYPE,
                                    new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(
                                        new AllocateEmptyPrimaryAllocationCommand(
                                            index,
                                            shardRouting.shardId().id(),
                                            otherNode.node.getName(),
                                            true
                                        )
                                    ),
                                    ActionListener.noop()
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
            return SnapshotsInProgress.get(master.clusterService.state()).isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        assertTrue(SnapshotsInProgress.get(testClusterNodes.randomDataNodeSafe().clusterService.state()).isEmpty());
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

        final SubscribableListener<SearchResponse> searchResponseStepListener = new SubscribableListener<>();

        continueOrDie(restoreSnapshotResponseStepListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
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
                    ((Map<?, ?>) masterNode.clusterService.state()
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
        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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
        final List<String> snapshotNames = IntStream.range(1, randomIntBetween(2, 4)).mapToObj(i -> "snapshot-" + i).toList();
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(1, 100);

        final TestClusterNodes.TestClusterNode masterNode = testClusterNodes.currentMaster(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
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
        assertTrue(masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).isEmpty());
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

    public void testSnapshotCompletedByNodeLeft() {

        // A transport interceptor that throttles the shard snapshot status updates to run one at a time, for more interesting interleavings
        final TransportInterceptor throttlingInterceptor = new TransportInterceptor() {
            private final ThrottledTaskRunner runner = new ThrottledTaskRunner(
                SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME + "-throttle",
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
                if (action.equals(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME)) {
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
        final var client = masterNode.client;
        final var masterClusterService = masterNode.clusterService;

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
                    if (action.equals(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME)) {
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
        final var client = masterNode.client;
        final var masterClusterService = masterNode.clusterService;

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
            SnapshotsService.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsService.class.getCanonicalName(),
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
            SnapshotsService.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsService.class.getCanonicalName(),
                Level.INFO,
                Strings.format("*failed to create snapshot*%s", expectedMessage)
            ),
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsService.class.getCanonicalName(),
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
            SnapshotsService.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsService.class.getCanonicalName(),
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
            SnapshotsService.class,
            new MockLog.SeenEventExpectation(
                "INFO log",
                SnapshotsService.class.getCanonicalName(),
                Level.INFO,
                Strings.format("*failed to create snapshot*other feature states were requested: [none, none]", "")
            )
        );
    }

    private RepositoryData getRepositoryData(Repository repository) {
        final PlainActionFuture<RepositoryData> res = new PlainActionFuture<>();
        repository.getRepositoryData(deterministicTaskQueue::scheduleNow, res);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(res.isDone());
        return res.actionGet();
    }

    private SubscribableListener<CreateIndexResponse> createRepoAndIndex(String repoName, String index, int shards) {
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
                .toList();
            final Set<String> masterNodeIds = clusterStates.stream()
                .map(clusterState -> clusterState.nodes().getMasterNodeId())
                .collect(Collectors.toSet());
            final Set<Long> terms = clusterStates.stream().map(ClusterState::term).collect(Collectors.toSet());
            final List<Long> versions = clusterStates.stream().map(ClusterState::version).distinct().toList();
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
        setupTestCluster(masterNodes, dataNodes, ignored -> TransportService.NOOP_TRANSPORT_INTERCEPTOR);
    }

    private void setupTestCluster(
        int masterNodes,
        int dataNodes,
        TestClusterNodes.TransportInterceptorFactory transportInterceptorFactory
    ) {
        testClusterNodes = new TestClusterNodes(masterNodes, dataNodes, transportInterceptorFactory);
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
        return indexSettings(shards, 0).build();
    }

    private static <T> void continueOrDie(SubscribableListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.addListener(ActionTestUtils.assertNoFailureListener(onResponse));
    }

    public NodeClient client() {
        // Select from sorted list of nodes
        final List<TestClusterNodes.TestClusterNode> nodes = testClusterNodes.nodes.values()
            .stream()
            .filter(n -> testClusterNodes.disconnectedNodes.contains(n.node.getName()) == false)
            .sorted(Comparator.comparing(n -> n.node.getName()))
            .toList();
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

        TestClusterNodes(int masterNodes, int dataNodes, TransportInterceptorFactory transportInterceptorFactory) {
            for (int i = 0; i < masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return newMasterNode(nodeName, transportInterceptorFactory);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
            for (int i = 0; i < dataNodes; ++i) {
                nodes.computeIfAbsent("data-node" + i, nodeName -> {
                    try {
                        return newDataNode(nodeName, transportInterceptorFactory);
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

        private TestClusterNode newMasterNode(String nodeName, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.MASTER_ROLE, transportInterceptorFactory);
        }

        private TestClusterNode newDataNode(String nodeName, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.DATA_ROLE, transportInterceptorFactory);
        }

        private TestClusterNode newNode(String nodeName, DiscoveryNodeRole role, TransportInterceptorFactory transportInterceptorFactory)
            throws IOException {
            return new TestClusterNode(
                DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).name(nodeName).roles(Collections.singleton(role)).build(),
                transportInterceptorFactory
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
                .toList();
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
                .toList();
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

        interface TransportInterceptorFactory {
            TransportInterceptor createTransportInterceptor(DiscoveryNode node);
        }

        private final class TestClusterNode {

            private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Stream.concat(ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream()).toList()
            );

            private final TransportInterceptorFactory transportInterceptorFactory;

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

            private final UsageService usageService;

            private Coordinator coordinator;

            TestClusterNode(DiscoveryNode node, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
                this.node = node;
                this.transportInterceptorFactory = transportInterceptorFactory;
                final Environment environment = createEnvironment(node.getName());
                threadPool = deterministicTaskQueue.getThreadPool(runnable -> DeterministicTaskQueue.onNodeLog(node, runnable));
                masterService = new FakeThreadPoolMasterService(node.getName(), threadPool, deterministicTaskQueue::scheduleNow);
                final Settings settings = environment.settings();
                client = new NodeClient(settings, threadPool);
                this.usageService = new UsageService();
                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterService = new ClusterService(
                    settings,
                    clusterSettings,
                    masterService,
                    new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                        @Override
                        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                            return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor(command -> new Runnable() {
                                @Override
                                public void run() {
                                    try (
                                        var ignored = DeterministicTaskQueue.getLogContext('{' + node.getName() + "}{" + node.getId() + '}')
                                    ) {
                                        command.run();
                                    }
                                }

                                @Override
                                public String toString() {
                                    return "TestClusterNode.ClusterApplierService[" + command + "]";
                                }
                            });
                        }

                        @Override
                        protected void connectToNodesAndWait(ClusterState newClusterState) {
                            connectToNodesAsync(newClusterState, () -> {
                                // no need to block waiting for handshakes etc. to complete, it's enough to let the NodeConnectionsService
                                // take charge of these connections
                            });
                        }
                    }
                );
                recoverySettings = new RecoverySettings(settings, clusterSettings);
                mockTransport = new DisruptableMockTransport(node, deterministicTaskQueue) {
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

                    @Override
                    public RecyclerBytesStreamOutput newNetworkBytesStream() {
                        // skip leak checks in these tests since they do indeed leak
                        return new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
                        // TODO fix these leaks and implement leak checking
                    }
                };
                transportService = mockTransport.createTransportService(
                    settings,
                    threadPool,
                    transportInterceptorFactory.createTransportInterceptor(node),
                    a -> node,
                    null,
                    emptySet()
                );
                IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
                bigArrays = new BigArrays(new PageCacheRecycler(settings), null, "test");
                repositoriesService = new RepositoriesService(
                    settings,
                    clusterService,
                    Collections.singletonMap(
                        FsRepository.TYPE,
                        (projectId, metadata) -> new FsRepository(
                            projectId,
                            metadata,
                            environment,
                            xContentRegistry(),
                            clusterService,
                            bigArrays,
                            recoverySettings
                        )
                    ),
                    emptyMap(),
                    threadPool,
                    client,
                    List.of()
                );
                final ActionFilters actionFilters = new ActionFilters(emptySet());
                snapshotsService = new SnapshotsService(
                    settings,
                    clusterService,
                    (reason, priority, listener) -> listener.onResponse(null),
                    indexNameExpressionResolver,
                    repositoriesService,
                    transportService,
                    actionFilters,
                    EmptySystemIndices.INSTANCE
                );
                nodeEnv = new NodeEnvironment(settings, environment);
                final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
                final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap(), () -> 1L);

                final SetOnce<RerouteService> rerouteServiceSetOnce = new SetOnce<>();
                final SnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                    settings,
                    clusterService,
                    repositoriesService,
                    rerouteServiceSetOnce::get
                );
                allocationService = ESAllocationTestCase.createAllocationService(
                    Settings.builder()
                        .put(settings)
                        .put("cluster.routing.allocation.type", "balanced") // TODO fix for desired_balance
                        .build(),
                    snapshotsInfoService
                );
                assertCriticalWarnings(
                    "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release. "
                        + "See the breaking changes documentation for the next major version."
                );
                rerouteService = new BatchedRerouteService(clusterService, allocationService::reroute);
                rerouteServiceSetOnce.set(rerouteService);
                final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
                    settings,
                    IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
                );
                final MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();

                indicesService = new IndicesServiceBuilder().settings(settings)
                    .pluginsService(mock(PluginsService.class))
                    .nodeEnvironment(nodeEnv)
                    .xContentRegistry(namedXContentRegistry)
                    .analysisRegistry(
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
                        )
                    )
                    .indexNameExpressionResolver(indexNameExpressionResolver)
                    .mapperRegistry(mapperRegistry)
                    .namedWriteableRegistry(namedWriteableRegistry)
                    .threadPool(threadPool)
                    .indexScopedSettings(indexScopedSettings)
                    .circuitBreakerService(new NoneCircuitBreakerService())
                    .bigArrays(bigArrays)
                    .scriptService(scriptService)
                    .clusterService(clusterService)
                    .projectResolver(DefaultProjectResolver.INSTANCE)
                    .client(client)
                    .metaStateService(new MetaStateService(nodeEnv, namedXContentRegistry))
                    .mapperMetrics(MapperMetrics.NOOP)
                    .build();
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
                Map<ActionType<?>, TransportAction<?, ?>> actions = new HashMap<>();
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
                actions.put(
                    VerifyNodeRepositoryAction.TYPE,
                    new VerifyNodeRepositoryAction.TransportAction(
                        transportService,
                        actionFilters,
                        threadPool,
                        clusterService,
                        repositoriesService
                    )
                );
                actions.put(
                    VerifyNodeRepositoryCoordinationAction.TYPE,
                    new VerifyNodeRepositoryCoordinationAction.LocalAction(actionFilters, transportService, clusterService, client)
                );
                final MetadataMappingService metadataMappingService = new MetadataMappingService(clusterService, indicesService);

                peerRecoverySourceService = new PeerRecoverySourceService(
                    transportService,
                    indicesService,
                    clusterService,
                    recoverySettings,
                    PeerOnlyRecoveryPlannerService.INSTANCE
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
                    new NoneCircuitBreakerService(),
                    EmptySystemIndices.INSTANCE.getExecutorSelector(),
                    Tracer.NOOP,
                    OnlinePrewarmingService.NOOP
                );

                final SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(repositoriesService);
                indicesClusterStateService = new IndicesClusterStateService(
                    settings,
                    indicesService,
                    clusterService,
                    threadPool,
                    new PeerRecoveryTargetService(
                        client,
                        threadPool,
                        transportService,
                        recoverySettings,
                        clusterService,
                        snapshotFilesProvider
                    ),
                    shardStateAction,
                    repositoriesService,
                    searchService,
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
                            EmptySystemIndices.INSTANCE,
                            TestProjectResolvers.DEFAULT_PROJECT_ONLY
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
                    shardLimitValidator,
                    environment,
                    indexScopedSettings,
                    threadPool,
                    namedXContentRegistry,
                    EmptySystemIndices.INSTANCE,
                    false,
                    new IndexSettingProviders(Set.of())
                );
                actions.put(
                    TransportCreateIndexAction.TYPE,
                    new TransportCreateIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataCreateIndexService,
                        actionFilters,
                        EmptySystemIndices.INSTANCE,
                        DefaultProjectResolver.INSTANCE
                    )
                );
                final MappingUpdatedAction mappingUpdatedAction = new MappingUpdatedAction(settings, clusterSettings);
                final IndexingPressure indexingMemoryLimits = new IndexingPressure(settings);
                mappingUpdatedAction.setClient(client);
                actions.put(
                    TransportBulkAction.TYPE,
                    new TransportBulkAction(
                        threadPool,
                        transportService,
                        clusterService,
                        new IngestService(
                            clusterService,
                            threadPool,
                            environment,
                            scriptService,
                            new AnalysisModule(environment, Collections.emptyList(), new StablePluginsRegistry()).getAnalysisRegistry(),
                            Collections.emptyList(),
                            client,
                            null,
                            FailureStoreMetrics.NOOP,
                            TestProjectResolvers.alwaysThrow(),
                            new FeatureService(List.of()) {
                                @Override
                                public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                                    return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                                }
                            }
                        ),
                        client,
                        actionFilters,
                        indexNameExpressionResolver,
                        new IndexingPressure(settings),
                        EmptySystemIndices.INSTANCE,
                        TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                        FailureStoreMetrics.NOOP,
                        DataStreamFailureStoreSettings.create(ClusterSettings.createBuiltInClusterSettings()),
                        new FeatureService(List.of()) {
                            @Override
                            public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                                return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                            }
                        }
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
                    EmptySystemIndices.INSTANCE,
                    TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                    DocumentParsingProvider.EMPTY_INSTANCE
                );
                actions.put(TransportShardBulkAction.TYPE, transportShardBulkAction);
                final RestoreService restoreService = new RestoreService(
                    clusterService,
                    repositoriesService,
                    allocationService,
                    metadataCreateIndexService,
                    new IndexMetadataVerifier(
                        settings,
                        clusterService,
                        namedXContentRegistry,
                        mapperRegistry,
                        indexScopedSettings,
                        ScriptCompiler.NONE,
                        MapperMetrics.NOOP
                    ),
                    shardLimitValidator,
                    EmptySystemIndices.INSTANCE,
                    indicesService,
                    mock(FileSettingsService.class),
                    threadPool
                );
                actions.put(
                    TransportPutMappingAction.TYPE,
                    new TransportPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new RequestValidators<>(Collections.emptyList()),
                        EmptySystemIndices.INSTANCE,
                        TestProjectResolvers.DEFAULT_PROJECT_ONLY
                    )
                );
                actions.put(
                    TransportAutoPutMappingAction.TYPE,
                    new TransportAutoPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                        EmptySystemIndices.INSTANCE
                    )
                );

                SearchPhaseController searchPhaseController = new SearchPhaseController(searchService::aggReduceContextBuilder);
                actions.put(
                    TransportSearchAction.TYPE,
                    new TransportSearchAction(
                        threadPool,
                        new NoneCircuitBreakerService(),
                        transportService,
                        searchService,
                        responseCollectorService,
                        searchTransportService,
                        searchPhaseController,
                        clusterService,
                        actionFilters,
                        TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                        indexNameExpressionResolver,
                        namedWriteableRegistry,
                        EmptySystemIndices.INSTANCE.getExecutorSelector(),
                        new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
                        client,
                        usageService
                    )
                );
                actions.put(
                    TransportRestoreSnapshotAction.TYPE,
                    new TransportRestoreSnapshotAction(transportService, clusterService, threadPool, restoreService, actionFilters)
                );
                actions.put(
                    TransportDeleteIndexAction.TYPE,
                    new TransportDeleteIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        new MetadataDeleteIndexService(settings, clusterService, allocationService),
                        actionFilters,
                        TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                        indexNameExpressionResolver,
                        new DestructiveOperations(settings, clusterSettings)
                    )
                );
                actions.put(
                    TransportPutRepositoryAction.TYPE,
                    new TransportPutRepositoryAction(transportService, clusterService, repositoriesService, threadPool, actionFilters)
                );
                actions.put(
                    TransportCleanupRepositoryAction.TYPE,
                    new TransportCleanupRepositoryAction(transportService, clusterService, repositoriesService, threadPool, actionFilters)
                );
                actions.put(
                    TransportCreateSnapshotAction.TYPE,
                    new TransportCreateSnapshotAction(transportService, clusterService, threadPool, snapshotsService, actionFilters)
                );
                actions.put(
                    TransportCloneSnapshotAction.TYPE,
                    new TransportCloneSnapshotAction(transportService, clusterService, threadPool, snapshotsService, actionFilters)
                );
                actions.put(
                    TransportGetSnapshotsAction.TYPE,
                    new TransportGetSnapshotsAction(transportService, clusterService, threadPool, repositoriesService, actionFilters)
                );
                actions.put(
                    TransportClusterRerouteAction.TYPE,
                    new TransportClusterRerouteAction(
                        transportService,
                        clusterService,
                        threadPool,
                        allocationService,
                        actionFilters,
                        TestProjectResolvers.alwaysThrow()
                    )
                );
                actions.put(
                    ClusterStateAction.INSTANCE,
                    new TransportClusterStateAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        DefaultProjectResolver.INSTANCE
                    )
                );
                actions.put(
                    TransportIndicesShardStoresAction.TYPE,
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
                    TransportDeleteSnapshotAction.TYPE,
                    new TransportDeleteSnapshotAction(transportService, clusterService, threadPool, snapshotsService, actionFilters)
                );
                client.initialize(
                    actions,
                    transportService.getTaskManager(),
                    () -> clusterService.localNode().getId(),
                    transportService.getLocalNodeConnection(),
                    transportService.getRemoteClusterService()
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
                            DiscoveryNodeUtils.create(node.getName(), node.getId(), node.getAddress(), emptyMap(), node.getRoles()),
                            transportInterceptorFactory
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
                    null,
                    namedWriteableRegistry,
                    allocationService,
                    masterService,
                    () -> persistedState,
                    hostsResolver -> nodes.values().stream().filter(n -> n.node.isMasterNode()).map(n -> n.node.getAddress()).toList(),
                    clusterService.getClusterApplierService(),
                    Collections.emptyList(),
                    random(),
                    rerouteService,
                    ElectionStrategy.DEFAULT_INSTANCE,
                    () -> new StatusInfo(HEALTHY, "healthy-info"),
                    new NoneCircuitBreakerService(),
                    new Reconfigurator(clusterService.getSettings(), clusterService.getClusterSettings()),
                    LeaderHeartbeatService.NO_OP,
                    StatefulPreVoteCollector::new,
                    CompatibilityVersionsUtils.staticCurrent(),
                    new FeatureService(List.of())
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

    private static <T> T safeResult(SubscribableListener<T> listener) {
        assertTrue("listener is not complete", listener.isDone());
        return safeAwait(listener);
    }
}
