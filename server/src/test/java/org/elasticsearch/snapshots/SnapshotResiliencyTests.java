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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.CoordinatorTests;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class SnapshotResiliencyTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    private TestClusterNodes testClusterNodes;

    private Path tempDir;

    @Before
    public void createServices() {
        tempDir = createTempDir();
        deterministicTaskQueue =
            new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "shared").build(), random());
    }

    @After
    public void verifyReposThenStopServices() throws IOException {
        try {
            assertNoStaleRepositoryData();
        } finally {
            testClusterNodes.nodes.values().forEach(TestClusterNode::stop);
        }
    }

    public void testSuccessfulSnapshotAndRestore() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(0, 100);
        TestClusterNode masterNode =
            testClusterNodes.currentMaster(testClusterNodes.nodes.values().iterator().next().clusterService.state());
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        final AtomicBoolean snapshotRestored = new AtomicBoolean();
        final AtomicBoolean documentCountVerified = new AtomicBoolean();
        masterNode.client.admin().cluster().preparePutRepository(repoName)
            .setType(FsRepository.TYPE).setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(
                assertNoFailureListener(
                    () -> masterNode.client.admin().indices().create(
                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL)
                            .settings(defaultIndexSettings(shards)),
                        assertNoFailureListener(
                            () -> {
                                final Runnable afterIndexing = () ->
                                    masterNode.client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                                        .setWaitForCompletion(true).execute(assertNoFailureListener(() -> {
                                        createdSnapshot.set(true);
                                        masterNode.client.admin().indices().delete(
                                            new DeleteIndexRequest(index),
                                            assertNoFailureListener(() -> masterNode.client.admin().cluster().restoreSnapshot(
                                                new RestoreSnapshotRequest(repoName, snapshotName).waitForCompletion(true),
                                                assertNoFailureListener(restoreSnapshotResponse -> {
                                                    snapshotRestored.set(true);
                                                    assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
                                                    masterNode.client.search(
                                                        new SearchRequest(index).source(
                                                            new SearchSourceBuilder().size(0).trackTotalHits(true)
                                                        ),
                                                        assertNoFailureListener(r -> {
                                                            assertEquals(
                                                                (long) documents,
                                                                Objects.requireNonNull(r.getHits().getTotalHits()).value
                                                            );
                                                            documentCountVerified.set(true);
                                                        }));
                                                })
                                            )));
                                    }));
                                final AtomicInteger countdown = new AtomicInteger(documents);
                                for (int i = 0; i < documents; ++i) {
                                    masterNode.client.bulk(
                                        new BulkRequest().add(new IndexRequest(index).source(
                                            Collections.singletonMap("foo", "bar" + i)))
                                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                                        assertNoFailureListener(
                                            bulkResponse -> {
                                                assertFalse(
                                                    "Failures in bulkresponse: " + bulkResponse.buildFailureMessage(),
                                                    bulkResponse.hasFailures());
                                                if (countdown.decrementAndGet() == 0) {
                                                    afterIndexing.run();
                                                }
                                            }));
                                }
                                if (documents == 0) {
                                    afterIndexing.run();
                                }
                            }))));
        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));
        assertTrue(createdSnapshot.get());
        assertTrue(snapshotRestored.get());
        assertTrue(documentCountVerified.get());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testSnapshotWithNodeDisconnects() {
        final int dataNodes = randomIntBetween(2, 10);
        setupTestCluster(randomFrom(1, 3, 5), dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);

        TestClusterNode masterNode =
            testClusterNodes.currentMaster(testClusterNodes.nodes.values().iterator().next().clusterService.state());
        final AtomicBoolean createdSnapshot = new AtomicBoolean();

        final AdminClient masterAdminClient = masterNode.client.admin();
        masterNode.client.admin().cluster().preparePutRepository(repoName)
            .setType(FsRepository.TYPE).setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(
                assertNoFailureListener(
                    () -> masterNode.client.admin().indices().create(
                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL)
                            .settings(defaultIndexSettings(shards)),
                        assertNoFailureListener(
                            () -> {
                                for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                                    scheduleNow(this::disconnectRandomDataNode);
                                }
                                if (randomBoolean()) {
                                    scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
                                }
                                masterAdminClient.cluster().prepareCreateSnapshot(repoName, snapshotName)
                                    .execute(assertNoFailureListener(() -> {
                                        for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                                            scheduleNow(this::disconnectOrRestartDataNode);
                                        }
                                        final boolean disconnectedMaster = randomBoolean();
                                        if (disconnectedMaster) {
                                            scheduleNow(this::disconnectOrRestartMasterNode);
                                        }
                                        if (disconnectedMaster || randomBoolean()) {
                                            scheduleSoon(() -> testClusterNodes.clearNetworkDisruptions());
                                        } else if (randomBoolean()) {
                                            scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
                                        }
                                        createdSnapshot.set(true);
                                    }));
                            }))));

        runUntil(() -> {
            final Optional<TestClusterNode> randomMaster = testClusterNodes.randomMasterNode();
            if (randomMaster.isPresent()) {
                final SnapshotsInProgress snapshotsInProgress = randomMaster.get().clusterService.state().custom(SnapshotsInProgress.TYPE);
                return snapshotsInProgress != null && snapshotsInProgress.entries().isEmpty();
            }
            return false;
        }, TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        final TestClusterNode randomMaster = testClusterNodes.randomMasterNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active master node"));
        SnapshotsInProgress finalSnapshotsInProgress = randomMaster.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = randomMaster.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));
    }

    public void testConcurrentSnapshotCreateAndDelete() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);

        TestClusterNode masterNode =
            testClusterNodes.currentMaster(testClusterNodes.nodes.values().iterator().next().clusterService.state());
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        masterNode.client.admin().cluster().preparePutRepository(repoName)
            .setType(FsRepository.TYPE).setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(
                assertNoFailureListener(
                    () -> masterNode.client.admin().indices().create(
                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL)
                            .settings(defaultIndexSettings(shards)),
                        assertNoFailureListener(
                            () -> masterNode.client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                                .execute(assertNoFailureListener(
                                    () -> masterNode.client.admin().cluster().deleteSnapshot(
                                        new DeleteSnapshotRequest(repoName, snapshotName),
                                        assertNoFailureListener(() -> masterNode.client.admin().cluster()
                                            .prepareCreateSnapshot(repoName, snapshotName).execute(
                                                assertNoFailureListener(() -> createdSnapshot.set(true))
                                            )))))))));

        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(createdSnapshot.get());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
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

        final TestClusterNode masterNode =
            testClusterNodes.currentMaster(testClusterNodes.nodes.values().iterator().next().clusterService.state());
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        final AdminClient masterAdminClient = masterNode.client.admin();
        masterAdminClient.cluster().preparePutRepository(repoName)
            .setType(FsRepository.TYPE).setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(
                assertNoFailureListener(
                    () -> masterAdminClient.indices().create(
                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL)
                            .settings(defaultIndexSettings(shards)),
                        assertNoFailureListener(
                            () -> masterAdminClient.cluster().state(new ClusterStateRequest(), assertNoFailureListener(
                                clusterStateResponse -> {
                                    final ShardRouting shardToRelocate =
                                        clusterStateResponse.getState().routingTable().allShards(index).get(0);
                                    final TestClusterNode currentPrimaryNode =
                                        testClusterNodes.nodeById(shardToRelocate.currentNodeId());
                                    final TestClusterNode otherNode =
                                        testClusterNodes.randomDataNodeSafe(currentPrimaryNode.node.getName());
                                    final Runnable maybeForceAllocate = new Runnable() {
                                        @Override
                                        public void run() {
                                            masterAdminClient.cluster().state(new ClusterStateRequest(), assertNoFailureListener(
                                                resp -> {
                                                    final ShardRouting shardRouting = resp.getState().routingTable()
                                                        .shardRoutingTable(shardToRelocate.shardId()).primaryShard();
                                                    if (shardRouting.unassigned()
                                                        && shardRouting.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_LEFT) {
                                                        if (masterNodeCount > 1) {
                                                            scheduleNow(() -> testClusterNodes.stopNode(masterNode));
                                                        }
                                                        testClusterNodes.randomDataNodeSafe().client.admin().cluster()
                                                            .prepareCreateSnapshot(repoName, snapshotName)
                                                            .execute(ActionListener.wrap(() -> {
                                                                testClusterNodes.randomDataNodeSafe().client.admin().cluster()
                                                                    .deleteSnapshot(
                                                                        new DeleteSnapshotRequest(repoName, snapshotName), noopListener());
                                                                createdSnapshot.set(true);
                                                            }));
                                                        scheduleNow(
                                                            () -> testClusterNodes.randomMasterNodeSafe().client.admin().cluster().reroute(
                                                                new ClusterRerouteRequest().add(
                                                                    new AllocateEmptyPrimaryAllocationCommand(
                                                                        index, shardRouting.shardId().id(), otherNode.node.getName(), true)
                                                                ), noopListener()));
                                                    } else {
                                                        scheduleSoon(this);
                                                    }
                                                }
                                            ));
                                        }
                                    };
                                    scheduleNow(() -> testClusterNodes.stopNode(currentPrimaryNode));
                                    scheduleNow(maybeForceAllocate);
                                }
                            ))))));

        runUntil(() -> {
            final Optional<TestClusterNode> randomMaster = testClusterNodes.randomMasterNode();
            if (randomMaster.isPresent()) {
                final SnapshotsInProgress snapshotsInProgress =
                    randomMaster.get().clusterService.state().custom(SnapshotsInProgress.TYPE);
                return (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) && createdSnapshot.get();
            }
            return false;
        }, TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        final SnapshotsInProgress finalSnapshotsInProgress = testClusterNodes.randomDataNodeSafe()
            .clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, either(hasSize(1)).or(hasSize(0)));
    }

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     */
    private void assertNoStaleRepositoryData() throws IOException {
        final Path repoPath = tempDir.resolve("repo").toAbsolutePath();
        final List<Path> repos;
        try (Stream<Path> reposDir = repoFilesByPrefix(repoPath)) {
            repos = reposDir.filter(s -> s.getFileName().toString().startsWith("extra") == false).collect(Collectors.toList());
        }
        for (Path repoRoot : repos) {
            cleanupEmptyTrees(repoRoot);
            final Path latestIndexGenBlob = repoRoot.resolve("index.latest");
            assertTrue("Could not find index.latest blob for repo at [" + repoRoot + ']', Files.exists(latestIndexGenBlob));
            final long latestGen = ByteBuffer.wrap(Files.readAllBytes(latestIndexGenBlob)).getLong(0);
            assertIndexGenerations(repoRoot, latestGen);
            final RepositoryData repositoryData;
            try (XContentParser parser =
                     XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                         new BytesArray(Files.readAllBytes(repoRoot.resolve("index-" + latestGen))), XContentType.JSON)) {
                repositoryData = RepositoryData.snapshotsFromXContent(parser, latestGen);
            }
            assertIndexUUIDs(repoRoot, repositoryData);
            assertSnapshotUUIDs(repoRoot, repositoryData);
        }
    }

    // Lucene's mock file system randomly generates empty `extra0` files that break the deletion of blob-store directories.
    // We clean those up here before checking a blob-store for stale files.
    private void cleanupEmptyTrees(Path repoPath) {
        try {
            Files.walkFileTree(repoPath, new SimpleFileVisitor<>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().startsWith("extra")) {
                        Files.delete(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    try {
                        Files.delete(dir);
                    } catch (DirectoryNotEmptyException e) {
                        // We're only interested in deleting empty trees here, just ignore directories with content
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertIndexGenerations(Path repoRoot, long latestGen) throws IOException {
        try (Stream<Path> repoRootBlobs = repoFilesByPrefix(repoRoot)) {
            final long[] indexGenerations = repoRootBlobs.filter(p -> p.getFileName().toString().startsWith("index-"))
                .map(p -> p.getFileName().toString().replace("index-", ""))
                .mapToLong(Long::parseLong).sorted().toArray();
            assertEquals(latestGen, indexGenerations[indexGenerations.length - 1]);
            assertTrue(indexGenerations.length <= 2);
        }
    }

    private static void assertIndexUUIDs(Path repoRoot, RepositoryData repositoryData) throws IOException {
        final List<String> expectedIndexUUIDs =
            repositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toList());
        try (Stream<Path> indexRoots = repoFilesByPrefix(repoRoot.resolve("indices"))) {
            final List<String> foundIndexUUIDs = indexRoots.filter(s -> s.getFileName().toString().startsWith("extra") == false)
                .map(p -> p.getFileName().toString()).collect(Collectors.toList());
            assertThat(foundIndexUUIDs, containsInAnyOrder(expectedIndexUUIDs.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    private static void assertSnapshotUUIDs(Path repoRoot, RepositoryData repositoryData) throws IOException {
        final List<String> expectedSnapshotUUIDs =
            repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toList());
        for (String prefix : new String[]{"snap-", "meta-"}) {
            try (Stream<Path> repoRootBlobs = repoFilesByPrefix(repoRoot)) {
                final Collection<String> foundSnapshotUUIDs = repoRootBlobs.filter(p -> p.getFileName().toString().startsWith(prefix))
                    .map(p -> p.getFileName().toString().replace(prefix, "").replace(".dat", ""))
                    .collect(Collectors.toSet());
                assertThat(foundSnapshotUUIDs, containsInAnyOrder(expectedSnapshotUUIDs.toArray(Strings.EMPTY_ARRAY)));
            }
        }
    }

    /**
     * List contents of a blob path and return an empty stream if the path doesn't exist.
     * @param prefix Path to find children for
     * @return stream of child paths
     * @throws IOException on failure
     */
    private static Stream<Path> repoFilesByPrefix(Path prefix) throws IOException {
        try {
            return Files.list(prefix);
        } catch (FileNotFoundException | NoSuchFileException e) {
            return Stream.empty();
        }
    }

    private void clearDisruptionsAndAwaitSync() {
        testClusterNodes.clearNetworkDisruptions();
        runUntil(() -> {
            final List<Long> versions = testClusterNodes.nodes.values().stream()
                .map(n -> n.clusterService.state().version()).distinct().collect(Collectors.toList());
            return versions.size() == 1L;
        }, TimeUnit.MINUTES.toMillis(1L));
    }

    private void disconnectOrRestartDataNode() {
        if (randomBoolean()) {
            disconnectRandomDataNode();
        } else {
            testClusterNodes.randomDataNode().ifPresent(TestClusterNode::restart);
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
        final ClusterState initialClusterState =
            new ClusterState.Builder(ClusterName.DEFAULT).nodes(testClusterNodes.discoveryNodes()).build();
        testClusterNodes.nodes.values().forEach(testClusterNode -> testClusterNode.start(initialClusterState));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        final VotingConfiguration votingConfiguration = new VotingConfiguration(testClusterNodes.nodes.values().stream().map(n -> n.node)
                .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode()).forEach(
            testClusterNode -> testClusterNode.coordinator.setInitialConfiguration(votingConfiguration));

        runUntil(
            () -> {
                List<String> masterNodeIds = testClusterNodes.nodes.values().stream()
                    .map(node -> node.clusterService.state().nodes().getMasterNodeId())
                    .distinct().collect(Collectors.toList());
                return masterNodeIds.size() == 1 && masterNodeIds.contains(null) == false;
            },
            TimeUnit.SECONDS.toMillis(30L)
        );
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
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shards)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0).build();
    }

    private static <T> ActionListener<T> assertNoFailureListener(Consumer<T> consumer) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(final T t) {
                consumer.accept(t);
            }

            @Override
            public void onFailure(final Exception e) {
                throw new AssertionError(e);
            }
        };
    }

    private static <T> ActionListener<T> assertNoFailureListener(Runnable r) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(final T t) {
                r.run();
            }

            @Override
            public void onFailure(final Exception e) {
                throw new AssertionError(e);
            }
        };
    }

    private static <T> ActionListener<T> noopListener() {
        return new ActionListener<T>() {
            @Override
            public void onResponse(final T t) {
            }

            @Override
            public void onFailure(final Exception e) {
            }
        };
    }

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private Environment createEnvironment(String nodeName) {
        return TestEnvironment.newEnvironment(Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), nodeName)
            .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
            .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY))
            .build());
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private final class TestClusterNodes {

        // LinkedHashMap so we have deterministic ordering when iterating over the map in tests
        private final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();

        private DisconnectedNodes disruptedLinks = new DisconnectedNodes();

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
            return nodes.values().stream().filter(n -> n.node.getId().equals(nodeId)).findFirst()
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
                new DiscoveryNode(nodeName, randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(),
                    Collections.singleton(role), Version.CURRENT), this::getDisruption);
        }

        public TestClusterNode randomMasterNodeSafe() {
            return randomMasterNode().orElseThrow(() -> new AssertionError("Expected to find at least one connected master node"));
        }

        public Optional<TestClusterNode> randomMasterNode() {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> masterNodes = testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode())
                .sorted(Comparator.comparing(n -> n.node.getName())).collect(Collectors.toList());
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
            final List<TestClusterNode> dataNodes = testClusterNodes.nodes.values().stream().filter(n -> n.node.isDataNode())
                .filter(n -> {
                    for (final String nodeName : excludedNames) {
                        if (n.node.getName().equals(nodeName)) {
                            return false;
                        }
                    }
                    return true;
                })
                .sorted(Comparator.comparing(n -> n.node.getName())).collect(Collectors.toList());
            return dataNodes.isEmpty() ? Optional.empty() : Optional.ofNullable(randomFrom(dataNodes));
        }

        public void disconnectNode(TestClusterNode node) {
            if (disruptedLinks.disconnected.contains(node.node.getName())) {
                return;
            }
            testClusterNodes.nodes.values().forEach(n -> n.transportService.getConnectionManager().disconnectFromNode(node.node));
            disruptedLinks.disconnect(node.node.getName());
        }

        public void clearNetworkDisruptions() {
            disruptedLinks.disconnected.forEach(nodeName -> {
                if (testClusterNodes.nodes.containsKey(nodeName)) {
                    final DiscoveryNode node = testClusterNodes.nodes.get(nodeName).node;
                    testClusterNodes.nodes.values().forEach(n -> n.transportService.getConnectionManager().openConnection(node, null));
                }
            });
            disruptedLinks.clear();
        }

        private NetworkDisruption.DisruptedLinks getDisruption() {
            return disruptedLinks;
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
    }

    private final class TestClusterNode {

        private final Logger logger = LogManager.getLogger(TestClusterNode.class);

        private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Stream.concat(
            ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream()).collect(Collectors.toList()));

        private final TransportService transportService;

        private final ClusterService clusterService;

        private final RepositoriesService repositoriesService;

        private final SnapshotsService snapshotsService;

        private final SnapshotShardsService snapshotShardsService;

        private final IndicesService indicesService;

        private final IndicesClusterStateService indicesClusterStateService;

        private final DiscoveryNode node;

        private final MasterService masterService;

        private final AllocationService allocationService;

        private final NodeClient client;

        private final NodeEnvironment nodeEnv;

        private final DisruptableMockTransport mockTransport;

        private final ThreadPool threadPool;

        private final Supplier<NetworkDisruption.DisruptedLinks> disruption;

        private Coordinator coordinator;

        TestClusterNode(DiscoveryNode node, Supplier<NetworkDisruption.DisruptedLinks> disruption) throws IOException {
            this.disruption = disruption;
            this.node = node;
            final Environment environment = createEnvironment(node.getName());
            masterService = new FakeThreadPoolMasterService(node.getName(), "test", deterministicTaskQueue::scheduleNow);
            final Settings settings = environment.settings();
            final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool = deterministicTaskQueue.getThreadPool();
            clusterService = new ClusterService(settings, clusterSettings, masterService,
                new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                    @Override
                    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                        return new MockSinglePrioritizingExecutor(node.getName(), deterministicTaskQueue);
                    }

                    @Override
                    protected void connectToNodesAndWait(ClusterState newClusterState) {
                        // don't do anything, and don't block
                    }
                });
            mockTransport = new DisruptableMockTransport(node, logger) {
                @Override
                protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                    return disruption.get().disrupt(node.getName(), destination.getName())
                        ? ConnectionStatus.DISCONNECTED : ConnectionStatus.CONNECTED;
                }

                @Override
                protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                    return testClusterNodes.nodes.values().stream().map(cn -> cn.mockTransport)
                        .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                        .findAny();
                }

                @Override
                protected void execute(Runnable runnable) {
                    scheduleNow(CoordinatorTests.onNodeLog(getLocalNode(), runnable));
                }

                @Override
                protected NamedWriteableRegistry writeableRegistry() {
                    return namedWriteableRegistry;
                }
            };
            transportService = mockTransport.createTransportService(
                settings, deterministicTaskQueue.getThreadPool(runnable -> CoordinatorTests.onNodeLog(node, runnable)),
                new TransportInterceptor() {
                    @Override
                    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                        boolean forceExecution, TransportRequestHandler<T> actualHandler) {
                        // TODO: Remove this hack once recoveries are async and can be used in these tests
                        if (action.startsWith("internal:index/shard/recovery")) {
                            return (request, channel, task) -> scheduleSoon(
                                new AbstractRunnable() {
                                    @Override
                                    protected void doRun() throws Exception {
                                        channel.sendResponse(new TransportException(new IOException("failed to recover shard")));
                                    }

                                    @Override
                                    public void onFailure(final Exception e) {
                                        throw new AssertionError(e);
                                    }
                                });
                        } else {
                            return actualHandler;
                        }
                    }
                },
                a -> node, null, emptySet()
            );
            final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
            repositoriesService = new RepositoriesService(
                settings, clusterService, transportService,
                Collections.singletonMap(FsRepository.TYPE, metaData -> {
                        final Repository repository = new FsRepository(metaData, environment, xContentRegistry(), threadPool) {
                            @Override
                            protected void assertSnapshotOrGenericThread() {
                                // eliminate thread name check as we create repo in the test thread
                            }
                        };
                        repository.start();
                        return repository;
                    }
                ),
                emptyMap(),
                threadPool
            );
            snapshotsService =
                new SnapshotsService(settings, clusterService, indexNameExpressionResolver, repositoriesService, threadPool);
            nodeEnv = new NodeEnvironment(settings, environment);
            final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
            final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap());
            client = new NodeClient(settings, threadPool);
            allocationService = ESAllocationTestCase.createAllocationService(settings);
            final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
            final BigArrays bigArrays = new BigArrays(new PageCacheRecycler(settings), null, "test");
            final MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
            indicesService = new IndicesService(
                settings,
                mock(PluginsService.class),
                nodeEnv,
                namedXContentRegistry,
                new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                    emptyMap(), emptyMap(), emptyMap(), emptyMap()),
                indexNameExpressionResolver,
                mapperRegistry,
                namedWriteableRegistry,
                threadPool,
                indexScopedSettings,
                new NoneCircuitBreakerService(),
                bigArrays,
                scriptService,
                client,
                new MetaStateService(nodeEnv, namedXContentRegistry),
                Collections.emptyList(),
                emptyMap()
            );
            final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
            final ActionFilters actionFilters = new ActionFilters(emptySet());
            snapshotShardsService = new SnapshotShardsService(
                settings, clusterService, snapshotsService, threadPool,
                transportService, indicesService, actionFilters, indexNameExpressionResolver);
            final ShardStateAction shardStateAction = new ShardStateAction(
                clusterService, transportService, allocationService,
                new RoutingService(clusterService, allocationService),
                threadPool
            );
            final MetaDataMappingService metaDataMappingService = new MetaDataMappingService(clusterService, indicesService);
            indicesClusterStateService = new IndicesClusterStateService(
                settings,
                indicesService,
                clusterService,
                threadPool,
                new PeerRecoveryTargetService(threadPool, transportService, recoverySettings, clusterService),
                shardStateAction,
                new NodeMappingRefreshAction(transportService, metaDataMappingService),
                repositoriesService,
                mock(SearchService.class),
                new SyncedFlushService(indicesService, clusterService, transportService, indexNameExpressionResolver),
                new PeerRecoverySourceService(transportService, indicesService, recoverySettings),
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
                        indexNameExpressionResolver)),
                new GlobalCheckpointSyncAction(
                    settings,
                    transportService,
                    clusterService,
                    indicesService,
                    threadPool,
                    shardStateAction,
                    actionFilters,
                    indexNameExpressionResolver),
                new RetentionLeaseSyncAction(
                    settings,
                    transportService,
                    clusterService,
                    indicesService,
                    threadPool,
                    shardStateAction,
                    actionFilters,
                    indexNameExpressionResolver),
                    new RetentionLeaseBackgroundSyncAction(
                            settings,
                            transportService,
                            clusterService,
                            indicesService,
                            threadPool,
                            shardStateAction,
                            actionFilters,
                            indexNameExpressionResolver));
            Map<Action, TransportAction> actions = new HashMap<>();
            final MetaDataCreateIndexService metaDataCreateIndexService = new MetaDataCreateIndexService(settings, clusterService,
                indicesService,
                allocationService, new AliasValidator(), environment, indexScopedSettings,
                threadPool, namedXContentRegistry, false);
            actions.put(CreateIndexAction.INSTANCE,
                new TransportCreateIndexAction(
                    transportService, clusterService, threadPool,
                    metaDataCreateIndexService,
                    actionFilters, indexNameExpressionResolver
                ));
            final MappingUpdatedAction mappingUpdatedAction = new MappingUpdatedAction(settings, clusterSettings);
            mappingUpdatedAction.setClient(client);
            final TransportShardBulkAction transportShardBulkAction = new TransportShardBulkAction(settings, transportService,
                clusterService, indicesService, threadPool, shardStateAction, mappingUpdatedAction, new UpdateHelper(scriptService),
                actionFilters, indexNameExpressionResolver);
            actions.put(BulkAction.INSTANCE,
                new TransportBulkAction(threadPool, transportService, clusterService,
                    new IngestService(
                        clusterService, threadPool, environment, scriptService,
                        new AnalysisModule(environment, Collections.emptyList()).getAnalysisRegistry(),
                        Collections.emptyList()),
                    transportShardBulkAction, client, actionFilters, indexNameExpressionResolver,
                    new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver)
                ));
            final RestoreService restoreService = new RestoreService(
                clusterService, repositoriesService, allocationService,
                metaDataCreateIndexService,
                new MetaDataIndexUpgradeService(
                    settings, namedXContentRegistry,
                    mapperRegistry,
                    indexScopedSettings,
                    Collections.emptyList()
                ),
                clusterSettings
            );
            actions.put(PutMappingAction.INSTANCE,
                new TransportPutMappingAction(transportService, clusterService, threadPool, metaDataMappingService,
                    actionFilters, indexNameExpressionResolver, new RequestValidators<>(Collections.emptyList())));
            final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
            final SearchTransportService searchTransportService = new SearchTransportService(transportService,
                SearchExecutionStatsCollector.makeWrapper(responseCollectorService));
            final SearchService searchService = new SearchService(clusterService, indicesService, threadPool, scriptService,
                bigArrays, new FetchPhase(Collections.emptyList()), responseCollectorService);
            actions.put(SearchAction.INSTANCE,
                new TransportSearchAction(threadPool, transportService, searchService,
                    searchTransportService, new SearchPhaseController(searchService::createReduceContext), clusterService,
                    actionFilters, indexNameExpressionResolver));
            actions.put(RestoreSnapshotAction.INSTANCE,
                new TransportRestoreSnapshotAction(transportService, clusterService, threadPool, restoreService, actionFilters,
                    indexNameExpressionResolver));
            actions.put(DeleteIndexAction.INSTANCE,
                new TransportDeleteIndexAction(
                    transportService, clusterService, threadPool,
                    new MetaDataDeleteIndexService(settings, clusterService, allocationService), actionFilters,
                    indexNameExpressionResolver, new DestructiveOperations(settings, clusterSettings)));
            actions.put(PutRepositoryAction.INSTANCE,
                new TransportPutRepositoryAction(
                    transportService, clusterService, repositoriesService, threadPool,
                    actionFilters, indexNameExpressionResolver
                ));
            actions.put(CreateSnapshotAction.INSTANCE,
                new TransportCreateSnapshotAction(
                    transportService, clusterService, threadPool,
                    snapshotsService, actionFilters, indexNameExpressionResolver
                ));
            actions.put(ClusterRerouteAction.INSTANCE,
                new TransportClusterRerouteAction(transportService, clusterService, threadPool, allocationService,
                    actionFilters, indexNameExpressionResolver));
            actions.put(ClusterStateAction.INSTANCE,
                new TransportClusterStateAction(transportService, clusterService, threadPool,
                    actionFilters, indexNameExpressionResolver));
            actions.put(IndicesShardStoresAction.INSTANCE,
                new TransportIndicesShardStoresAction(
                    transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                    new TransportNodesListGatewayStartedShards(settings,
                        threadPool, clusterService, transportService, actionFilters, nodeEnv, indicesService, namedXContentRegistry))
            );
            actions.put(DeleteSnapshotAction.INSTANCE,
                new TransportDeleteSnapshotAction(
                    transportService, clusterService, threadPool,
                    snapshotsService, actionFilters, indexNameExpressionResolver
                ));
            client.initialize(actions, () -> clusterService.localNode().getId(), transportService.getRemoteClusterService());
        }

        public void restart() {
            testClusterNodes.disconnectNode(this);
            final ClusterState oldState = this.clusterService.state();
            stop();
            testClusterNodes.nodes.remove(node.getName());
            scheduleSoon(() -> {
                try {
                    final TestClusterNode restartedNode = new TestClusterNode(
                        new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), emptyMap(),
                            node.getRoles(), Version.CURRENT), disruption);
                    testClusterNodes.nodes.put(node.getName(), restartedNode);
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
            indicesClusterStateService.close();
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
            final CoordinationState.PersistedState persistedState =
                new InMemoryPersistedState(initialState.term(), stateForNode(initialState, node));
            coordinator = new Coordinator(node.getName(), clusterService.getSettings(),
                clusterService.getClusterSettings(), transportService, namedWriteableRegistry,
                allocationService, masterService, () -> persistedState,
                hostsResolver -> testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode())
                    .map(n -> n.node.getAddress()).collect(Collectors.toList()),
                clusterService.getClusterApplierService(), Collections.emptyList(), random(),
                new RoutingService(clusterService, allocationService)::reroute);
            masterService.setClusterStatePublisher(coordinator);
            coordinator.start();
            masterService.start();
            clusterService.getClusterApplierService().setNodeConnectionsService(
                new NodeConnectionsService(clusterService.getSettings(), threadPool, transportService));
            clusterService.getClusterApplierService().start();
            indicesService.start();
            indicesClusterStateService.start();
            coordinator.startInitialJoin();
        }
    }

    private final class DisconnectedNodes extends NetworkDisruption.DisruptedLinks {

        /**
         * Node names that are disconnected from all other nodes.
         */
        private final Set<String> disconnected = new HashSet<>();

        @Override
        public boolean disrupt(String node1, String node2) {
            if (node1.equals(node2)) {
                return false;
            }
            // Check if both nodes are still part of the cluster
            if (testClusterNodes.nodes.containsKey(node1) == false
                || testClusterNodes.nodes.containsKey(node2) == false) {
                return true;
            }
            return disconnected.contains(node1) || disconnected.contains(node2);
        }

        public void disconnect(String node) {
            disconnected.add(node);
        }

        public void clear() {
            disconnected.clear();
        }
    }
}
