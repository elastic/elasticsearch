/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.DelayRecoveryException;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoverySnapshotFileRequest;
import org.elasticsearch.indices.recovery.RecoverySourceHandler;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SnapshotBasedIndexRecoveryIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ConfigurableMockSnapshotBasedRecoveriesPlugin.class,
            TestRepositoryPlugin.class,
            MockTransportService.TestPlugin.class,
            InternalSettingsPlugin.class
        );
    }

    @After
    public void clearRepoDelegate() {
        FilterFsRepository.clearReadBlobWrapper();
    }

    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {
        public static final String FAULTY_TYPE = "faultyrepo";
        public static final String INSTRUMENTED_TYPE = "instrumentedrepo";
        public static final String FILTER_TYPE = "filterrepo";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                FAULTY_TYPE,
                metadata -> new FaultyRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings),
                INSTRUMENTED_TYPE,
                metadata -> new InstrumentedRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings),
                FILTER_TYPE,
                metadata -> new FilterFsRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    public static class InstrumentedRepo extends FsRepository {
        AtomicLong totalBytesRead = new AtomicLong();

        public InstrumentedRepo(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        public BlobContainer shardContainer(IndexId indexId, int shardId) {
            return new FilterBlobContainer(super.shardContainer(indexId, shardId)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public InputStream readBlob(String blobName) throws IOException {
                    // Take into account only index files
                    if (blobName.startsWith("__") == false) {
                        return super.readBlob(blobName);
                    }

                    return new FilterInputStream(super.readBlob(blobName)) {
                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            int read = super.read(b, off, len);
                            if (read > 0) {
                                totalBytesRead.addAndGet(read);
                            }
                            return read;
                        }
                    };
                }
            };
        }
    }

    public static class FaultyRepository extends FsRepository {
        public FaultyRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        public BlobContainer shardContainer(IndexId indexId, int shardId) {
            return new FilterBlobContainer(super.shardContainer(indexId, shardId)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public InputStream readBlob(String blobName) throws IOException {
                    // Fail only in index files
                    if (blobName.startsWith("__") == false) {
                        return super.readBlob(blobName);
                    }

                    return new FilterInputStream(super.readBlob(blobName)) {
                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            if (randomBoolean()) {
                                // Return random data
                                for (int i = 0; i < len; i++) {
                                    b[off + i] = randomByte();
                                }
                                return len;
                            } else {
                                if (randomBoolean()) {
                                    throw new IOException("Unable to read blob " + blobName);
                                } else {
                                    // Skip some file chunks
                                    int read = super.read(b, off, len);
                                    return read / 2;
                                }
                            }
                        }
                    };
                }
            };
        }
    }

    public static class FilterFsRepository extends FsRepository {
        static final BiFunction<String, InputStream, InputStream> IDENTITY = (blobName, inputStream) -> inputStream;
        static final AtomicReference<BiFunction<String, InputStream, InputStream>> delegateSupplierRef = new AtomicReference<>(IDENTITY);

        public FilterFsRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        static void wrapReadBlobMethod(BiFunction<String, InputStream, InputStream> delegate) {
            delegateSupplierRef.set(delegate);
        }

        static void clearReadBlobWrapper() {
            delegateSupplierRef.set(IDENTITY);
        }

        @Override
        public BlobContainer shardContainer(IndexId indexId, int shardId) {
            return new FilterBlobContainer(super.shardContainer(indexId, shardId)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public InputStream readBlob(String blobName) throws IOException {
                    BiFunction<String, InputStream, InputStream> delegateSupplier = delegateSupplierRef.get();
                    return delegateSupplier.apply(blobName, super.readBlob(blobName));
                }
            };
        }
    }

    public void testPeerRecoveryUsesSnapshots() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.require._name", sourceNode)
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        String snapshot = "snap";
        createSnapshot(repoName, snapshot, Collections.singletonList(indexName));

        String targetNode = internalCluster().startDataOnlyNode();

        MockTransportService sourceMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            sourceNode
        );
        MockTransportService targetMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            targetNode
        );

        sourceMockTransportService.addSendBehavior(targetMockTransportService, (connection, requestId, action, request, options) -> {
            assertNotEquals(PeerRecoveryTargetService.Actions.FILE_CHUNK, action);
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, targetNode);
        InstrumentedRepo repository = (InstrumentedRepo) repositoriesService.repository(repoName);

        // segments_N and .si files are recovered from the file metadata directly
        long expectedRecoveredBytesFromRepo = 0;
        long totalBytesRecoveredFromSnapshot = 0;
        for (RecoveryState.FileDetail fileDetail : recoveryState.getIndex().fileDetails()) {
            totalBytesRecoveredFromSnapshot += fileDetail.recoveredFromSnapshot();
            if (fileDetail.name().startsWith("segments") || fileDetail.name().endsWith(".si")) {
                continue;
            }
            expectedRecoveredBytesFromRepo += fileDetail.recovered();
        }

        assertThat(repository.totalBytesRead.get(), is(equalTo(expectedRecoveredBytesFromRepo)));

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, snapshot, indexName);
        assertThat(repository.totalBytesRead.get(), is(greaterThan(0L)));
        assertThat(repository.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));
        assertThat(totalBytesRecoveredFromSnapshot, is(equalTo(snapshotSizeForIndex)));

        assertDocumentsAreEqual(indexName, numDocs);
    }

    @TestLogging(reason = "testing logging on failure", value = "org.elasticsearch.indices.recovery.RecoverySourceHandler:WARN")
    public void testFallbacksToSourceNodeWhenSnapshotDownloadFails() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.require._name", sourceNode)
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.FAULTY_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        String targetNode;
        final var recoverySourceHandlerLogger = LogManager.getLogger(RecoverySourceHandler.class);
        final var mockLogAppender = new MockLogAppender();
        mockLogAppender.start();
        try {
            Loggers.addAppender(recoverySourceHandlerLogger, mockLogAppender);
            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warn log about restore failure",
                    RecoverySourceHandler.class.getName(),
                    Level.WARN,
                    "failed to recover file [*] from snapshot, will recover from primary instead"
                )
            );

            targetNode = internalCluster().startDataOnlyNode();
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);

            ensureGreen();

            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(recoverySourceHandlerLogger, mockLogAppender);
            mockLogAppender.stop();
        }

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        assertDocumentsAreEqual(indexName, numDocs);
    }

    public void testRateLimitingIsEnforced() throws Exception {
        try {
            updateSetting(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "50k");

            String sourceNode = internalCluster().startDataOnlyNode();
            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(
                indexName,
                indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                    .put("index.routing.allocation.require._name", sourceNode)
                    .build()
            );

            // we theoretically only need more than 256 bytes, since SimpleRateLimiter.MIN_PAUSE_CHECK_MSEC=5.
            // We do need a bit more though to ensure we have enough time to handle if network and CI is generally slow,
            // since if the experienced download rate is less than 50KB there will be no throttling.
            // I would at least 4x that to be on a somewhat safe side against things like a single GC.
            int numDocs = randomIntBetween(1000, 2000);
            indexDocs(indexName, 0, numDocs);

            String repoName = "repo";
            createRepo(repoName, "fs");
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            String targetNode = internalCluster().startDataOnlyNode();
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);

            ensureGreen();

            RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
            assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

            assertDocumentsAreEqual(indexName, numDocs);

            NodesStatsResponse statsResponse = client().admin()
                .cluster()
                .prepareNodesStats()
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            for (NodeStats nodeStats : statsResponse.getNodes()) {
                RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                String nodeName = nodeStats.getNode().getName();
                if (nodeName.equals(sourceNode)) {
                    assertThat(recoveryStats.throttleTime().getMillis(), is(equalTo(0L)));
                }
                if (nodeName.equals(targetNode)) {
                    assertThat(recoveryStats.throttleTime().getMillis(), is(greaterThan(0L)));
                }
            }
        } finally {
            updateSetting(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), null);
        }
    }

    public void testPeerRecoveryTriesToUseMostOfTheDataFromAnAvailableSnapshot() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.require._name", sourceNode)
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);
        forceMerge();

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        int docsIndexedAfterSnapshot = randomIntBetween(1, 2000);
        indexDocs(indexName, numDocs, docsIndexedAfterSnapshot);

        String targetNode = internalCluster().startDataOnlyNode();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        InstrumentedRepo repository = getRepositoryOnNode(repoName, targetNode);

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);
        assertThat(repository.totalBytesRead.get(), is(greaterThan(0L)));
        assertThat(repository.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));

        assertDocumentsAreEqual(indexName, numDocs + docsIndexedAfterSnapshot);
    }

    public void testPeerRecoveryDoNotUseSnapshotsWhenSegmentsAreNotSharedAndSeqNosAreDifferent() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.require._name", sourceNode)
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        int docsIndexedAfterSnapshot = randomIntBetween(1, 2000);
        indexDocs(indexName, numDocs, docsIndexedAfterSnapshot);
        forceMerge();

        String targetNode = internalCluster().startDataOnlyNode();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        InstrumentedRepo repository = getRepositoryOnNode(repoName, targetNode);

        assertThat(repository.totalBytesRead.get(), is(equalTo(0L)));

        assertDocumentsAreEqual(indexName, numDocs + docsIndexedAfterSnapshot);
    }

    @TestLogging(reason = "testing logging on cancellation", value = "org.elasticsearch.indices.recovery.RecoverySourceHandler:DEBUG")
    public void testRecoveryIsCancelledAfterDeletingTheIndex() throws Exception {
        updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), "1");

        try {
            boolean seqNoRecovery = randomBoolean();
            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            final Settings.Builder indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s");

            final List<String> dataNodes;
            if (seqNoRecovery) {
                dataNodes = internalCluster().startDataOnlyNodes(3);
                indexSettings.put("index.routing.allocation.include._name", String.join(",", dataNodes))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
            } else {
                dataNodes = internalCluster().startDataOnlyNodes(1);
                indexSettings.put("index.routing.allocation.require._name", dataNodes.get(0))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            }
            createIndex(indexName, indexSettings.build());
            ensureGreen(indexName);

            int numDocs = randomIntBetween(300, 1000);
            indexDocs(indexName, numDocs, numDocs);
            if (seqNoRecovery) {
                // Flush to ensure that index_commit_seq_nos(replica) == index_commit_seq_nos(primary),
                // since the primary flushes the index before taking the snapshot.
                flush(indexName);
            }

            String repoName = "repo";
            createRepo(repoName, "fs");
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            final String targetNode;
            if (seqNoRecovery) {
                ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index(indexName).shard(0);
                String primaryNodeName = clusterState.nodes().resolveNode(shardRoutingTable.primaryShard().currentNodeId()).getName();
                String replicaNodeName = clusterState.nodes()
                    .resolveNode(shardRoutingTable.replicaShards().get(0).currentNodeId())
                    .getName();

                targetNode = dataNodes.stream()
                    .filter(nodeName -> nodeName.equals(primaryNodeName) == false && nodeName.equals(replicaNodeName) == false)
                    .findFirst()
                    .get();

            } else {
                targetNode = internalCluster().startDataOnlyNode();
            }

            MockTransportService targetMockTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                targetNode
            );

            CountDownLatch recoverSnapshotFileRequestReceived = new CountDownLatch(1);
            CountDownLatch respondToRecoverSnapshotFile = new CountDownLatch(1);
            AtomicInteger numberOfRecoverSnapshotFileRequestsReceived = new AtomicInteger();
            targetMockTransportService.addRequestHandlingBehavior(
                PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT,
                (handler, request, channel, task) -> {
                    assertThat(numberOfRecoverSnapshotFileRequestsReceived.incrementAndGet(), is(equalTo(1)));
                    recoverSnapshotFileRequestReceived.countDown();
                    respondToRecoverSnapshotFile.await();
                    handler.messageReceived(request, channel, task);
                }
            );

            if (seqNoRecovery) {
                ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index(indexName).shard(0);
                String primaryNodeName = clusterState.nodes().resolveNode(shardRoutingTable.primaryShard().currentNodeId()).getName();

                assertThat(internalCluster().stopNode(primaryNodeName), is(equalTo(true)));
            } else {
                updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexName);
            }

            recoverSnapshotFileRequestReceived.await();

            final var recoverySourceHandlerLogger = LogManager.getLogger(RecoverySourceHandler.class);
            final var mockLogAppender = new MockLogAppender();
            mockLogAppender.start();
            try {
                Loggers.addAppender(recoverySourceHandlerLogger, mockLogAppender);
                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "expected debug log about restore cancellation",
                        RecoverySourceHandler.class.getName(),
                        Level.DEBUG,
                        "cancelled while recovering file [*] from snapshot"
                    )
                );
                mockLogAppender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "expected no WARN logs",
                        RecoverySourceHandler.class.getName(),
                        Level.WARN,
                        "*"
                    )
                );

                assertAcked(client().admin().indices().prepareDelete(indexName).get());

                assertBusy(mockLogAppender::assertAllExpectationsMatched);
            } finally {
                Loggers.removeAppender(recoverySourceHandlerLogger, mockLogAppender);
                mockLogAppender.stop();
            }

            respondToRecoverSnapshotFile.countDown();

            assertThat(indexExists(indexName), is(equalTo(false)));
        } finally {
            updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), null);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/96427")
    public void testCancelledRecoveryAbortsDownloadPromptly() throws Exception {
        updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), "1");

        try {
            internalCluster().ensureAtLeastNumDataNodes(2);

            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(
                indexName,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            );
            ensureGreen(indexName);

            int numDocs = randomIntBetween(1, 1000);
            indexDocs(indexName, numDocs, numDocs);

            String repoName = "repo";
            createRepo(repoName, TestRepositoryPlugin.FILTER_TYPE);
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            final AtomicBoolean isCancelled = new AtomicBoolean();
            final CountDownLatch readFromBlobCalledLatch = new CountDownLatch(1);
            final CountDownLatch readFromBlobRespondLatch = new CountDownLatch(1);

            internalCluster().getInstances(TransportService.class)
                .forEach(
                    transportService -> ((MockTransportService) transportService).addRequestHandlingBehavior(
                        PeerRecoverySourceService.Actions.START_RECOVERY,
                        (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public String getChannelType() {
                                return channel.getChannelType();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                fail("recovery should not succeed");
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                // Must not respond until the index deletion is applied on the target node, or else it will get an
                                // IllegalIndexShardStateException which it considers to be retryable, and will reset the recovery and
                                // generate a new `CancellableThreads` which is cancelled instead of the original `CancellableThreads`,
                                // permitting a subsequent read.
                                transportService.getThreadPool().generic().execute(() -> {
                                    safeAwait(readFromBlobRespondLatch);
                                    try {
                                        channel.sendResponse(exception);
                                    } catch (IOException e) {
                                        throw new AssertionError("unexpected", e);
                                    }
                                });
                            }
                        }, task)
                    )
                );

            FilterFsRepository.wrapReadBlobMethod((blobName, stream) -> {
                if (blobName.startsWith("__")) {
                    return new FilterInputStream(stream) {
                        @Override
                        public int read() throws IOException {
                            beforeRead();
                            return super.read();
                        }

                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            beforeRead();
                            return super.read(b, off, len);
                        }

                        private void beforeRead() {
                            assertFalse(isCancelled.get()); // should have no further reads once the index is deleted
                            readFromBlobCalledLatch.countDown();
                            safeAwait(readFromBlobRespondLatch);
                        }
                    };
                } else {
                    return stream;
                }
            });

            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
            safeAwait(readFromBlobCalledLatch);

            assertAcked(client().admin().indices().prepareDelete(indexName).get());
            // cancellation flag is set when applying the cluster state that deletes the index, so no further waiting is necessary
            isCancelled.set(true);
            readFromBlobRespondLatch.countDown();

            assertThat(indexExists(indexName), is(equalTo(false)));
            assertBusy(
                () -> internalCluster().getInstances(PeerRecoveryTargetService.class)
                    .forEach(peerRecoveryTargetService -> assertEquals(0, peerRecoveryTargetService.ongoingRecoveryCount()))
            );
        } finally {
            updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), null);
        }
    }

    public void testRecoveryAfterRestoreUsesSnapshots() throws Exception {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        assertAcked(client().admin().indices().prepareDelete(indexName).get());

        List<String> restoredIndexDataNodes = internalCluster().startDataOnlyNodes(2);
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, "snap")
            .setIndices(indexName)
            .setIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", String.join(",", restoredIndexDataNodes))
            )
            .setWaitForCompletion(true)
            .get();

        RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(equalTo(restoreInfo.totalShards())));

        ensureGreen(indexName);
        assertDocumentsAreEqual(indexName, numDocs);

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        String sourceNode = recoveryState.getSourceNode().getName();
        String targetNode = recoveryState.getTargetNode().getName();

        assertThat(restoredIndexDataNodes.contains(sourceNode), is(equalTo(true)));
        assertThat(restoredIndexDataNodes.contains(targetNode), is(equalTo(true)));
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        // Since we did a restore first, and the index is static the data retrieved by the target node
        // via repository should be equal to the amount of data that the source node retrieved from the repo
        InstrumentedRepo sourceRepo = getRepositoryOnNode(repoName, sourceNode);
        InstrumentedRepo targetRepo = getRepositoryOnNode(repoName, targetNode);
        assertThat(sourceRepo.totalBytesRead.get(), is(equalTo(targetRepo.totalBytesRead.get())));

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);

        assertThat(sourceRepo.totalBytesRead.get(), is(greaterThan(0L)));
        assertThat(sourceRepo.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));
    }

    public void testReplicaRecoveryUsesSnapshots() throws Exception {
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        setReplicaCount(1, indexName);

        ensureGreen(indexName);
        assertDocumentsAreEqual(indexName, numDocs);

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        String currentPrimary = recoveryState.getSourceNode().getName();
        String replica = recoveryState.getTargetNode().getName();
        assertPeerRecoveryWasSuccessful(recoveryState, currentPrimary, replica);

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);

        InstrumentedRepo replicaRepo = getRepositoryOnNode(repoName, replica);
        assertThat(replicaRepo.totalBytesRead.get(), is(greaterThan(0L)));
        assertThat(replicaRepo.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));

        // Stop the current replica
        if (randomBoolean()) {
            internalCluster().stopNode(replica);

            ensureGreen(indexName);
            assertDocumentsAreEqual(indexName, numDocs);

            RecoveryState recoveryStateAfterReplicaFailure = getLatestPeerRecoveryStateForShard(indexName, 0);
            final String name = recoveryStateAfterReplicaFailure.getSourceNode().getName();
            final String newReplica = recoveryStateAfterReplicaFailure.getTargetNode().getName();
            assertPeerRecoveryWasSuccessful(recoveryStateAfterReplicaFailure, name, newReplica);

            InstrumentedRepo newReplicaRepo = getRepositoryOnNode(repoName, newReplica);
            assertThat(newReplicaRepo.totalBytesRead.get(), is(greaterThan(0L)));
            assertThat(newReplicaRepo.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));
        }
    }

    public void testDisabledSnapshotBasedRecoveryUsesSourceFiles() throws Exception {
        updateSetting(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), "false");

        try {
            checkRecoveryIsPerformedFromSourceNode();
        } finally {
            updateSetting(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), null);
        }
    }

    public void testRecoveryConcurrentlyWithIndexing() throws Exception {
        internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .build()
        );

        AtomicInteger numDocs = new AtomicInteger(randomIntBetween(1, 1000));
        indexDocs(indexName, 0, numDocs.get());

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);

        setReplicaCount(1, indexName);

        boolean waitForSnapshotDownloadToStart = randomBoolean();
        if (waitForSnapshotDownloadToStart) {
            // wait for the snapshot download to start.
            assertBusy(() -> {
                RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
                assertThat(recoveryState.getIndex().recoveredBytes(), greaterThan(0L));
            });
        }

        // busy wait to complete and add a bit of indexing.
        assertBusy(() -> {
            if (randomBoolean()) {
                int moreDocs = between(1, 5);
                indexDocs(indexName, numDocs.getAndAdd(moreDocs), moreDocs);
            }
            RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
            assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
        });

        ensureGreen(indexName);

        if (waitForSnapshotDownloadToStart) {
            // must complete using snapshots alone.
            RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
            assertThat(recoveryState.getIndex().recoveredFromSnapshotBytes(), equalTo(snapshotSizeForIndex));
        }

        assertDocumentsAreEqual(indexName, numDocs.get());
    }

    public void testSeqNoBasedRecoveryIsUsedAfterPrimaryFailOver() throws Exception {
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );
        ensureGreen(indexName);

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);
        // Flush to ensure that index_commit_seq_nos(replica) == index_commit_seq_nos(primary),
        // since the primary flushes the index before taking the snapshot.
        flush(indexName);

        String repoType = randomFrom(TestRepositoryPlugin.FAULTY_TYPE, TestRepositoryPlugin.INSTRUMENTED_TYPE, "fs");
        String repoName = "repo";
        createRepo(repoName, repoType);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String primaryNodeId = clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String primaryNodeName = clusterState.nodes().resolveNode(primaryNodeId).getName();

        Store.MetadataSnapshot primaryMetadataSnapshot = getMetadataSnapshot(primaryNodeName, indexName);

        assertThat(internalCluster().stopNode(primaryNodeName), is(equalTo(true)));

        ensureGreen(indexName);

        ClusterState clusterStateAfterPrimaryFailOver = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shardRoutingTableAfterFailOver = clusterStateAfterPrimaryFailOver.routingTable().index(indexName).shard(0);

        String primaryNodeIdAfterFailOver = shardRoutingTableAfterFailOver.primaryShard().currentNodeId();
        String primaryNodeNameAfterFailOver = clusterStateAfterPrimaryFailOver.nodes().resolveNode(primaryNodeIdAfterFailOver).getName();

        String replicaNodeIdAfterFailOver = shardRoutingTableAfterFailOver.replicaShards().get(0).currentNodeId();
        String replicaNodeNameAfterFailOver = clusterStateAfterPrimaryFailOver.nodes().resolveNode(replicaNodeIdAfterFailOver).getName();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, primaryNodeNameAfterFailOver, replicaNodeNameAfterFailOver);
        assertDocumentsAreEqual(indexName, numDocs);

        if (repoType.equals(TestRepositoryPlugin.FAULTY_TYPE) == false) {
            for (RecoveryState.FileDetail fileDetail : recoveryState.getIndex().fileDetails()) {
                assertThat(fileDetail.recoveredFromSnapshot(), is(equalTo(fileDetail.length())));
            }

            Store.MetadataSnapshot replicaAfterFailoverMetadataSnapshot = getMetadataSnapshot(replicaNodeNameAfterFailOver, indexName);
            Store.RecoveryDiff recoveryDiff = primaryMetadataSnapshot.recoveryDiff(replicaAfterFailoverMetadataSnapshot);
            assertThat(recoveryDiff.identical, is(not(empty())));
        }
    }

    public void testRecoveryUsingSnapshotsIsThrottledPerNode() throws Exception {
        executeRecoveryWithSnapshotFileDownloadThrottled(
            (
                indices,
                sourceNode,
                targetNode,
                targetMockTransportService,
                recoverySnapshotFileRequests,
                awaitForRecoverSnapshotFileRequestReceived,
                respondToRecoverSnapshotFile) -> {
                String indexRecoveredFromSnapshot1 = indices.get(0);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot1
                );

                awaitForRecoverSnapshotFileRequestReceived.run();

                // Ensure that peer recoveries can make progress without restoring snapshot files
                // while the permit is granted to a different recovery
                String indexRecoveredFromPeer = indices.get(1);
                updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexRecoveredFromPeer);

                ensureGreen(indexRecoveredFromPeer);
                assertPeerRecoveryDidNotUseSnapshots(indexRecoveredFromPeer, sourceNode, targetNode);

                // let snapshot file restore to proceed
                respondToRecoverSnapshotFile.run();

                ensureGreen(indexRecoveredFromSnapshot1);

                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot1, sourceNode, targetNode);

                for (RecoverySnapshotFileRequest recoverySnapshotFileRequest : recoverySnapshotFileRequests) {
                    String indexName = recoverySnapshotFileRequest.shardId().getIndexName();
                    assertThat(indexName, is(equalTo(indexRecoveredFromSnapshot1)));
                }

                targetMockTransportService.clearAllRules();

                String indexRecoveredFromSnapshot2 = indices.get(2);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot2
                );

                ensureGreen(indexRecoveredFromSnapshot2);

                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot2, sourceNode, targetNode);

            }
        );
    }

    public void testRecoveryUsingSnapshotsPermitIsReturnedAfterFailureOrCancellation() throws Exception {
        executeRecoveryWithSnapshotFileDownloadThrottled(
            (
                indices,
                sourceNode,
                targetNode,
                targetMockTransportService,
                recoverySnapshotFileRequests,
                awaitForRecoverSnapshotFileRequestReceived,
                respondToRecoverSnapshotFile) -> {
                String indexRecoveredFromSnapshot1 = indices.get(0);
                updateIndexSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put("index.routing.allocation.require._name", (String) null)
                        .put("index.routing.allocation.include._name", sourceNode + "," + targetNode),
                    indexRecoveredFromSnapshot1
                );

                awaitForRecoverSnapshotFileRequestReceived.run();

                targetMockTransportService.clearAllRules();

                boolean cancelRecovery = randomBoolean();
                if (cancelRecovery) {
                    assertAcked(client().admin().indices().prepareDelete(indexRecoveredFromSnapshot1).get());

                    respondToRecoverSnapshotFile.run();

                    assertThat(indexExists(indexRecoveredFromSnapshot1), is(equalTo(false)));
                } else {
                    // Recovery would fail and should release the granted permit and allow other
                    // recoveries to use snapshots
                    CountDownLatch cleanFilesRequestReceived = new CountDownLatch(1);
                    AtomicReference<TransportChannel> channelRef = new AtomicReference<>();
                    targetMockTransportService.addRequestHandlingBehavior(
                        PeerRecoveryTargetService.Actions.CLEAN_FILES,
                        (handler, request, channel, task) -> {
                            channelRef.compareAndExchange(null, channel);
                            cleanFilesRequestReceived.countDown();
                        }
                    );

                    respondToRecoverSnapshotFile.run();
                    cleanFilesRequestReceived.await();

                    targetMockTransportService.clearAllRules();
                    channelRef.get().sendResponse(new IOException("unable to clean files"));
                    PeerRecoveryTargetService peerRecoveryTargetService = internalCluster().getInstance(
                        PeerRecoveryTargetService.class,
                        targetNode
                    );
                    assertBusy(() -> {
                        // Wait until the current RecoveryTarget releases the snapshot download permit
                        try (Releasable snapshotDownloadPermit = peerRecoveryTargetService.tryAcquireSnapshotDownloadPermits()) {
                            assertThat(snapshotDownloadPermit, is(notNullValue()));
                        }
                    });
                }

                String indexRecoveredFromSnapshot2 = indices.get(1);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot2
                );

                ensureGreen(indexRecoveredFromSnapshot2);

                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot2, sourceNode, targetNode);
            }
        );
    }

    public void testRecoveryReEstablishKeepsTheGrantedSnapshotFileDownloadPermit() throws Exception {
        executeRecoveryWithSnapshotFileDownloadThrottled(
            (
                indices,
                sourceNode,
                targetNode,
                targetMockTransportService,
                recoverySnapshotFileRequests,
                awaitForRecoverSnapshotFileRequestReceived,
                respondToRecoverSnapshotFile) -> {
                AtomicReference<Transport.Connection> startRecoveryConnection = new AtomicReference<>();
                CountDownLatch reestablishRecoverySent = new CountDownLatch(1);
                targetMockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                        startRecoveryConnection.compareAndExchange(null, connection);
                    } else if (action.equals(PeerRecoverySourceService.Actions.REESTABLISH_RECOVERY)) {
                        reestablishRecoverySent.countDown();
                    }
                    connection.sendRequest(requestId, action, request, options);
                });

                String indexRecoveredFromSnapshot1 = indices.get(0);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot1
                );

                awaitForRecoverSnapshotFileRequestReceived.run();

                startRecoveryConnection.get().close();

                reestablishRecoverySent.await();

                String indexRecoveredFromPeer = indices.get(1);
                updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexRecoveredFromPeer);

                ensureGreen(indexRecoveredFromPeer);
                assertPeerRecoveryDidNotUseSnapshots(indexRecoveredFromPeer, sourceNode, targetNode);

                respondToRecoverSnapshotFile.run();

                ensureGreen(indexRecoveredFromSnapshot1);
                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot1, sourceNode, targetNode);

                targetMockTransportService.clearAllRules();

                final String indexRecoveredFromSnapshot2 = indices.get(2);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot2
                );

                ensureGreen(indexRecoveredFromSnapshot2);
                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot2, sourceNode, targetNode);
            }
        );
    }

    public void testRecoveryRetryKeepsTheGrantedSnapshotFileDownloadPermit() throws Exception {
        executeRecoveryWithSnapshotFileDownloadThrottled(
            (
                indices,
                sourceNode,
                targetNode,
                targetMockTransportService,
                recoverySnapshotFileRequests,
                awaitForRecoverSnapshotFileRequestReceived,
                respondToRecoverSnapshotFile) -> {
                MockTransportService sourceMockTransportService = (MockTransportService) internalCluster().getInstance(
                    TransportService.class,
                    sourceNode
                );

                CountDownLatch startRecoveryRetryReceived = new CountDownLatch(1);
                AtomicBoolean delayRecoveryExceptionSent = new AtomicBoolean();
                sourceMockTransportService.addRequestHandlingBehavior(
                    PeerRecoverySourceService.Actions.START_RECOVERY,
                    (handler, request, channel, task) -> {
                        if (delayRecoveryExceptionSent.compareAndSet(false, true)) {
                            channel.sendResponse(new DelayRecoveryException("delay"));
                        } else {
                            startRecoveryRetryReceived.countDown();
                            handler.messageReceived(request, channel, task);
                        }
                    }
                );

                String indexRecoveredFromSnapshot1 = indices.get(0);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot1
                );

                startRecoveryRetryReceived.await();
                sourceMockTransportService.clearAllRules();
                awaitForRecoverSnapshotFileRequestReceived.run();

                String indexRecoveredFromPeer = indices.get(1);
                updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", targetNode), indexRecoveredFromPeer);

                ensureGreen(indexRecoveredFromPeer);
                assertPeerRecoveryDidNotUseSnapshots(indexRecoveredFromPeer, sourceNode, targetNode);

                respondToRecoverSnapshotFile.run();

                ensureGreen(indexRecoveredFromSnapshot1);
                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot1, sourceNode, targetNode);

                targetMockTransportService.clearAllRules();

                final String indexRecoveredFromSnapshot2 = indices.get(2);
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.require._name", targetNode),
                    indexRecoveredFromSnapshot2
                );

                ensureGreen(indexRecoveredFromSnapshot2);
                assertPeerRecoveryUsedSnapshots(indexRecoveredFromSnapshot2, sourceNode, targetNode);
            }
        );
    }

    public void testNodeDisconnectsDoNotOverAccountRecoveredBytes() throws Exception {
        // This test reproduces a rare (but possible scenario) where a shard is recovering using
        // snapshots, using logically equivalent index files, but half-way the connection between
        // the source and the target drops.
        // - The target node keeps downloading the snapshot file
        // - The source aborts the snapshot based recovery
        // - This deletes the temporary files and resets the recovery state in the target
        // - The target updates the recovered bytes for the file it has been downloading, after the recovery state was cleared.
        // This could end up over-accounting the number of recovered bytes

        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );
        ensureGreen(indexName);

        updateClusterSettings(
            Settings.builder()
                // Do not retry the first RESTORE_FILE_FROM_SNAPSHOT after the connection is closed
                .put(INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), 1)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.getKey(), 1)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING.getKey(), 1)
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);
        // Flush to ensure that index_commit_seq_nos(replica) == index_commit_seq_nos(primary),
        // since the primary flushes the index before taking the snapshot.
        flush(indexName);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.FILTER_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String primaryNodeId = clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String primaryNodeName = clusterState.nodes().resolveNode(primaryNodeId).getName();
        String replicaNodeId = clusterState.routingTable().index(indexName).shard(0).replicaShards().get(0).currentNodeId();
        String replicaNodeName = clusterState.nodes().resolveNode(replicaNodeId).getName();

        String newReplicaNodeName = dataNodes.stream()
            .filter(nodeName -> nodeName.equals(primaryNodeName) == false)
            .filter(nodeName -> nodeName.equals(replicaNodeName) == false)
            .findFirst()
            .orElseThrow();

        MockTransportService sourceMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replicaNodeName
        );

        MockTransportService targetMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            newReplicaNodeName
        );

        final CountDownLatch firstDownloadStartLatch = new CountDownLatch(1);
        final CountDownLatch blockSnapshotFileDownload = new CountDownLatch(1);

        final AtomicBoolean firstDataBlobRead = new AtomicBoolean();
        FilterFsRepository.wrapReadBlobMethod((blobName, inputStream) -> {
            if (blobName.startsWith("__") && firstDataBlobRead.compareAndSet(false, true)) {
                return new FilterInputStream(inputStream) {
                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        firstDownloadStartLatch.countDown();
                        try {
                            blockSnapshotFileDownload.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return super.read(b, off, len);
                    }
                };
            } else {
                return inputStream;
            }
        });

        Set<Transport.Connection> connectionRefs = Collections.synchronizedSet(new HashSet<>());
        sourceMockTransportService.addSendBehavior(targetMockTransportService, (connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT)) {
                connectionRefs.add(connection);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        assertThat(internalCluster().stopNode(primaryNodeName), is(equalTo(true)));

        firstDownloadStartLatch.await();

        CountDownLatch firstFileChunkSent = new CountDownLatch(1);
        CountDownLatch blockFileChunkDownload = new CountDownLatch(1);

        targetMockTransportService.addRequestHandlingBehavior(
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            (handler, request, channel, task) -> {
                firstFileChunkSent.countDown();
                blockFileChunkDownload.await();
                handler.messageReceived(request, channel, task);
            }
        );

        // Increase the retry timeout as it takes a bit until the source node gets reconnected to the target node
        updateSetting(INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.getKey(), "1m");
        assertThat(connectionRefs, is(not(empty())));
        connectionRefs.forEach(Transport.Connection::close);

        firstFileChunkSent.await();

        blockSnapshotFileDownload.countDown();
        blockFileChunkDownload.countDown();

        ensureGreen(indexName);

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        for (RecoveryState.FileDetail fileDetail : recoveryState.getIndex().fileDetails()) {
            assertThat(fileDetail.length(), is(equalTo(fileDetail.recovered())));
        }
        IndexShard shard = internalCluster().getInstance(IndicesService.class, newReplicaNodeName)
            .indexServiceSafe(resolveIndex(indexName))
            .getShard(0);

        // Ensure that leftovers are eventually cleaned
        assertBusy(() -> {
            String[] indexFiles = shard.store().directory().listAll();
            assertThat(Arrays.toString(indexFiles), Arrays.stream(indexFiles).noneMatch(file -> file.startsWith("recovery.")), is(true));
        });
    }

    private void executeRecoveryWithSnapshotFileDownloadThrottled(SnapshotBasedRecoveryThrottlingTestCase testCase) throws Exception {
        updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), "1");
        updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), "1");

        try {
            List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
            List<String> indices = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                createIndex(
                    indexName,
                    indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                        .put("index.routing.allocation.require._name", dataNodes.get(0))
                        .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                        .build()
                );
                indices.add(indexName);
            }

            String repoName = "repo";
            createRepo(repoName, "fs");

            for (String indexName : indices) {
                int numDocs = randomIntBetween(300, 1000);
                indexDocs(indexName, numDocs, numDocs);

                createSnapshot(repoName, "snap-" + indexName, Collections.singletonList(indexName));
            }

            String sourceNode = dataNodes.get(0);
            String targetNode = dataNodes.get(1);
            MockTransportService targetMockTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                targetNode
            );

            List<RecoverySnapshotFileRequest> recoverySnapshotFileRequests = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch recoverSnapshotFileRequestReceived = new CountDownLatch(1);
            CountDownLatch respondToRecoverSnapshotFile = new CountDownLatch(1);
            targetMockTransportService.addRequestHandlingBehavior(
                PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT,
                (handler, request, channel, task) -> {
                    recoverySnapshotFileRequests.add((RecoverySnapshotFileRequest) request);
                    recoverSnapshotFileRequestReceived.countDown();
                    respondToRecoverSnapshotFile.await();
                    handler.messageReceived(request, channel, task);
                }
            );

            testCase.execute(
                indices,
                sourceNode,
                targetNode,
                targetMockTransportService,
                recoverySnapshotFileRequests,
                recoverSnapshotFileRequestReceived::await,
                respondToRecoverSnapshotFile::countDown
            );
        } finally {
            updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), null);
            updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), null);
        }
    }

    public void testFallbacksToSourceNodeWhenLicenseIsInvalid() throws Exception {
        ConfigurableMockSnapshotBasedRecoveriesPlugin.denyRecoveryFromSnapshot(this::checkRecoveryIsPerformedFromSourceNode);
    }

    private void checkRecoveryIsPerformedFromSourceNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        setReplicaCount(1, indexName);

        ensureGreen(indexName);
        assertDocumentsAreEqual(indexName, numDocs);

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        String currentPrimary = recoveryState.getSourceNode().getName();
        String replica = recoveryState.getTargetNode().getName();
        assertPeerRecoveryWasSuccessful(recoveryState, currentPrimary, replica);

        InstrumentedRepo replicaRepo = getRepositoryOnNode(repoName, replica);
        assertThat(replicaRepo.totalBytesRead.get(), is(equalTo(0L)));
    }

    interface SnapshotBasedRecoveryThrottlingTestCase {
        void execute(
            List<String> indices,
            String sourceNode,
            String targetNode,
            MockTransportService targetMockTransportService,
            List<RecoverySnapshotFileRequest> recoverySnapshotFileRequests,
            CheckedRunnable<Exception> awaitForRecoverSnapshotFileRequestReceived,
            Runnable respondToRecoverSnapshotFile
        ) throws Exception;
    }

    private void assertPeerRecoveryUsedSnapshots(String indexName, String sourceNode, String targetNode) {
        RecoveryState recoveryStateIndexRecoveredFromPeer = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryStateIndexRecoveredFromPeer, sourceNode, targetNode);
        assertThat(recoveryStateIndexRecoveredFromPeer.getIndex().recoveredFromSnapshotBytes(), is(greaterThan(0L)));
    }

    private void assertPeerRecoveryDidNotUseSnapshots(String indexName, String sourceNode, String targetNode) {
        RecoveryState recoveryStateIndexRecoveredFromPeer = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryStateIndexRecoveredFromPeer, sourceNode, targetNode);
        assertThat(recoveryStateIndexRecoveredFromPeer.getIndex().recoveredFromSnapshotBytes(), is(equalTo(0L)));
    }

    private Store.MetadataSnapshot getMetadataSnapshot(String nodeName, String indexName) throws IOException {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexService(clusterState.metadata().index(indexName).getIndex());
        IndexShard shard = indexService.getShard(0);
        try (Engine.IndexCommitRef indexCommitRef = shard.acquireSafeIndexCommit()) {
            IndexCommit safeCommit = indexCommitRef.getIndexCommit();
            assertThat(safeCommit, is(notNullValue()));
            return shard.store().getMetadata(safeCommit);
        }
    }

    private long getSnapshotSizeForIndex(String repository, String snapshot, String index) {
        GetSnapshotsResponse getSnapshotsResponse = client().admin().cluster().prepareGetSnapshots(repository).addSnapshots(snapshot).get();
        for (SnapshotInfo snapshotInfo : getSnapshotsResponse.getSnapshots()) {
            SnapshotInfo.IndexSnapshotDetails indexSnapshotDetails = snapshotInfo.indexSnapshotDetails().get(index);
            assertThat(indexSnapshotDetails, is(notNullValue()));
            return indexSnapshotDetails.getSize().getBytes();
        }

        return -1;
    }

    private void indexDocs(String indexName, int docIdOffset, int docCount) throws Exception {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[docCount];
        for (int i = 0; i < builders.length; i++) {
            int docId = i + docIdOffset;
            builders[i] = client().prepareIndex(indexName)
                .setId(Integer.toString(docId))
                .setSource("field", docId, "field2", "Some text " + docId);
        }
        indexRandom(true, builders);

        // Ensure that the safe commit == latest commit
        assertBusy(() -> {
            ShardStats stats = client().admin()
                .indices()
                .prepareStats(indexName)
                .clear()
                .get()
                .asMap()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().shardId().getId() == 0)
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            assertThat(stats, is(notNullValue()));
            assertThat(stats.getSeqNoStats(), is(notNullValue()));

            assertThat(stats.getSeqNoStats().getMaxSeqNo(), is(greaterThan(-1L)));
            assertThat(stats.getSeqNoStats().getGlobalCheckpoint(), is(greaterThan(-1L)));
            assertThat(
                Strings.toString(stats.getSeqNoStats()),
                stats.getSeqNoStats().getMaxSeqNo(),
                equalTo(stats.getSeqNoStats().getGlobalCheckpoint())
            );
        }, 60, TimeUnit.SECONDS);
    }

    private void assertDocumentsAreEqual(String indexName, int docCount) {
        assertDocCount(indexName, docCount);
        for (int testCase = 0; testCase < 3; testCase++) {
            final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(indexName)
                .addSort("field", SortOrder.ASC)
                .setSize(10_000);

            SearchResponse searchResponse;
            switch (testCase) {
                case 0 -> {
                    searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).get();
                    assertSearchResponseContainsAllIndexedDocs(searchResponse, docCount);
                }
                case 1 -> {
                    int docIdToMatch = randomIntBetween(0, docCount - 1);
                    searchResponse = searchRequestBuilder.setQuery(QueryBuilders.termQuery("field", docIdToMatch)).get();
                    assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    SearchHit searchHit = searchResponse.getHits().getAt(0);
                    Map<String, Object> source = searchHit.getSourceAsMap();
                    assertThat(source, is(notNullValue()));
                    assertThat(source.get("field"), is(equalTo(docIdToMatch)));
                    assertThat(source.get("field2"), is(equalTo("Some text " + docIdToMatch)));
                }
                case 2 -> {
                    searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchQuery("field2", "text")).get();
                    assertSearchResponseContainsAllIndexedDocs(searchResponse, docCount);
                }
                default -> throw new IllegalStateException("Unexpected value: " + testCase);
            }
        }
    }

    private void assertSearchResponseContainsAllIndexedDocs(SearchResponse searchResponse, long docCount) {
        assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(docCount));
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            Map<String, Object> source = searchHit.getSourceAsMap();

            assertThat(source, is(notNullValue()));
            assertThat(source.get("field"), is(equalTo(i)));
            assertThat(source.get("field2"), is(equalTo("Some text " + i)));
        }
    }

    private void assertPeerRecoveryWasSuccessful(RecoveryState recoveryState, String sourceNode, String targetNode) {
        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
        assertThat(recoveryState.getRecoverySource(), equalTo(RecoverySource.PeerRecoverySource.INSTANCE));

        assertThat(recoveryState.getSourceNode(), notNullValue());
        assertThat(recoveryState.getSourceNode().getName(), equalTo(sourceNode));
        assertThat(recoveryState.getTargetNode(), notNullValue());
        assertThat(recoveryState.getTargetNode().getName(), equalTo(targetNode));

        RecoveryState.Index indexState = recoveryState.getIndex();
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    private RecoveryState getLatestPeerRecoveryStateForShard(String indexName, int shardId) {
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).get();
        assertThat(recoveryResponse.hasRecoveries(), equalTo(true));
        List<RecoveryState> indexRecoveries = recoveryResponse.shardRecoveryStates().get(indexName);
        assertThat(indexRecoveries, notNullValue());

        List<RecoveryState> peerRecoveries = indexRecoveries.stream()
            .filter(recoveryState -> recoveryState.getStage() == RecoveryState.Stage.DONE)
            .filter(recoveryState -> recoveryState.getRecoverySource().equals(RecoverySource.PeerRecoverySource.INSTANCE))
            .filter(recoveryState -> recoveryState.getShardId().getId() == shardId)
            .sorted(Comparator.comparingLong(o -> o.getTimer().stopTime()))
            .collect(Collectors.toList());

        assertThat(peerRecoveries, is(not(empty())));
        return peerRecoveries.get(peerRecoveries.size() - 1);
    }

    private void updateSetting(String key, String value) {
        updateClusterSettings(Settings.builder().put(key, value));
    }

    private void createRepo(String repoName, String type) {
        final Settings.Builder settings = Settings.builder()
            .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
            .put("location", randomRepoPath());
        createRepository(logger, repoName, type, settings, true);
    }
}
