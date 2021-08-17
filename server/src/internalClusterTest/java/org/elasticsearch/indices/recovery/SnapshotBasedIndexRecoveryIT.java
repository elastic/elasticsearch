/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.recovery.RecoveryStats;
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
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
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
            TestRepositoryPlugin.class,
            MockTransportService.TestPlugin.class,
            InternalSettingsPlugin.class
        );
    }

    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {
        public static final String FAULTY_TYPE = "faultyrepo";
        public static final String INSTRUMENTED_TYPE = "instrumentedrepo";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return org.elasticsearch.core.Map.of(
                FAULTY_TYPE,
                metadata -> new FaultyRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings),
                INSTRUMENTED_TYPE,
                metadata -> new InstrumentedRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    public static class InstrumentedRepo extends FsRepository {
        AtomicLong totalBytesRead = new AtomicLong();

        public InstrumentedRepo(RepositoryMetadata metadata,
                                Environment environment,
                                NamedXContentRegistry namedXContentRegistry,
                                ClusterService clusterService,
                                BigArrays bigArrays,
                                RecoverySettings recoverySettings) {
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
        public FaultyRepository(RepositoryMetadata metadata,
                                Environment environment,
                                NamedXContentRegistry namedXContentRegistry,
                                ClusterService clusterService,
                                BigArrays bigArrays,
                                RecoverySettings recoverySettings) {
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

    public void testPeerRecoveryUsesSnapshots() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
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

        MockTransportService sourceMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, sourceNode);
        MockTransportService targetMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, targetNode);

        sourceMockTransportService.addSendBehavior(targetMockTransportService, (connection, requestId, action, request, options) -> {
            assertNotEquals(PeerRecoveryTargetService.Actions.FILE_CHUNK, action);
            connection.sendRequest(requestId, action, request, options);
        });

        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put("index.routing.allocation.require._name", targetNode)).get()
        );

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

    public void testFallbacksToSourceNodeWhenSnapshotDownloadFails() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.require._name", sourceNode)
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.FAULTY_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        String targetNode = internalCluster().startDataOnlyNode();
        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put("index.routing.allocation.require._name", targetNode)).get()
        );

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        assertDocumentsAreEqual(indexName, numDocs);
    }

    public void testRateLimitingIsEnforced() throws Exception {
        try {
            updateSetting(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "50k");

            String sourceNode = internalCluster().startDataOnlyNode();
            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                    .put("index.routing.allocation.require._name", sourceNode)
                    .build()
            );

            //we theoretically only need more than 256 bytes, since SimpleRateLimiter.MIN_PAUSE_CHECK_MSEC=5.
            // We do need a bit more though to ensure we have enough time to handle if network and CI is generally slow,
            // since if the experienced download rate is less than 50KB there will be no throttling.
            // I would at least 4x that to be on a somewhat safe side against things like a single GC.
            int numDocs = randomIntBetween(1000, 2000);
            indexDocs(indexName, 0, numDocs);

            String repoName = "repo";
            createRepo(repoName, "fs");
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            String targetNode = internalCluster().startDataOnlyNode();
            assertAcked(
                client().admin().indices().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder()
                        .put("index.routing.allocation.require._name", targetNode)).get()
            );

            ensureGreen();

            RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
            assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

            assertDocumentsAreEqual(indexName, numDocs);

            NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
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
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
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
        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put("index.routing.allocation.require._name", targetNode)).get()
        );

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        InstrumentedRepo repository = getRepositoryOnNode(repoName, targetNode);

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);
        assertThat(repository.totalBytesRead.get(), is(greaterThan(0L)));
        assertThat(repository.totalBytesRead.get(), is(lessThanOrEqualTo(snapshotSizeForIndex)));

        assertDocumentsAreEqual(indexName, numDocs + docsIndexedAfterSnapshot);
    }

    public void testPeerRecoveryDoNotUseSnapshotsWhenSegmentsAreNotShared() throws Exception {
        String sourceNode = internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
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
        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put("index.routing.allocation.require._name", targetNode)).get()
        );

        ensureGreen();

        RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
        assertPeerRecoveryWasSuccessful(recoveryState, sourceNode, targetNode);

        InstrumentedRepo repository = getRepositoryOnNode(repoName, targetNode);

        assertThat(repository.totalBytesRead.get(), is(equalTo(0L)));

        assertDocumentsAreEqual(indexName, numDocs + docsIndexedAfterSnapshot);
    }

    public void testRecoveryIsCancelledAfterDeletingTheIndex() throws Exception {
        updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), "1");

        try {
            String sourceNode = internalCluster().startDataOnlyNode();
            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                    .put("index.routing.allocation.require._name", sourceNode)
                    .build()
            );

            int numDocs = randomIntBetween(300, 1000);
            indexDocs(indexName, numDocs, numDocs);

            String repoName = "repo";
            createRepo(repoName, "fs");
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            String targetNode = internalCluster().startDataOnlyNode();
            assertAcked(
                client().admin().indices().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder()
                        .put("index.routing.allocation.require._name", targetNode)).get()
            );

            MockTransportService targetMockTransportService =
                (MockTransportService) internalCluster().getInstance(TransportService.class, targetNode);

            CountDownLatch recoverSnapshotFileRequestReceived = new CountDownLatch(1);
            CountDownLatch respondToRecoverSnapshotFile = new CountDownLatch(1);
            AtomicInteger numberOfRecoverSnapshotFileRequestsReceived = new AtomicInteger();
            targetMockTransportService.addRequestHandlingBehavior(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT,
                (handler, request, channel, task) -> {
                    assertThat(numberOfRecoverSnapshotFileRequestsReceived.incrementAndGet(), is(equalTo(1)));
                    recoverSnapshotFileRequestReceived.countDown();
                    respondToRecoverSnapshotFile.await();
                    handler.messageReceived(request, channel, task);
                }
            );

            recoverSnapshotFileRequestReceived.await();

            assertAcked(client().admin().indices().prepareDelete(indexName).get());

            respondToRecoverSnapshotFile.countDown();

            assertThat(indexExists(indexName), is(equalTo(false)));
        } finally {
            updateSetting(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), null);
        }
    }

    public void testRecoveryAfterRestoreUsesSnapshots() throws Exception {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
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
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster()
            .prepareRestoreSnapshot(repoName, "snap")
            .setIndices(indexName)
            .setIndexSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", String.join(",", restoredIndexDataNodes))
            ).setWaitForCompletion(true)
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
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );

        int numDocs = randomIntBetween(300, 1000);
        indexDocs(indexName, 0, numDocs);

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

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
            internalCluster().ensureAtLeastNumDataNodes(2);
            String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                    .build()
            );

            int numDocs = randomIntBetween(300, 1000);
            indexDocs(indexName, 0, numDocs);

            String repoName = "repo";
            createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
            createSnapshot(repoName, "snap", Collections.singletonList(indexName));

            assertAcked(
                client().admin().indices().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            );

            ensureGreen(indexName);
            assertDocumentsAreEqual(indexName, numDocs);

            RecoveryState recoveryState = getLatestPeerRecoveryStateForShard(indexName, 0);
            String currentPrimary = recoveryState.getSourceNode().getName();
            String replica = recoveryState.getTargetNode().getName();
            assertPeerRecoveryWasSuccessful(recoveryState, currentPrimary, replica);

            InstrumentedRepo replicaRepo = getRepositoryOnNode(repoName, replica);
            assertThat(replicaRepo.totalBytesRead.get(), is(equalTo(0L)));
        } finally {
            updateSetting(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), null);
        }
    }

    public void testRecoveryConcurrentlyWithIndexing() throws Exception {
        internalCluster().startDataOnlyNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .build()
        );

        AtomicInteger numDocs = new AtomicInteger(randomIntBetween(1, 1000));
        indexDocs(indexName, 0, numDocs.get());

        String repoName = "repo";
        createRepo(repoName, TestRepositoryPlugin.INSTRUMENTED_TYPE);
        createSnapshot(repoName, "snap", Collections.singletonList(indexName));

        long snapshotSizeForIndex = getSnapshotSizeForIndex(repoName, "snap", indexName);

        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

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

    private long getSnapshotSizeForIndex(String repository, String snapshot, String index) {
        GetSnapshotsResponse getSnapshotsResponse =
            client().admin().cluster().prepareGetSnapshots(repository).addSnapshots(snapshot).get();
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
            builders[i] = client().prepareIndex(indexName, "_doc")
                .setId(Integer.toString(docId))
                .setSource("field", docId, "field2", "Some text " + docId);
        }
        indexRandom(true, builders);

        // Ensure that the safe commit == latest commit
        assertBusy(() -> {
            ShardStats stats = client().admin().indices().prepareStats(indexName).clear().get()
                .asMap().entrySet().stream().filter(e -> e.getKey().shardId().getId() == 0)
                .map(Map.Entry::getValue).findFirst().orElse(null);
            assertThat(stats, is(notNullValue()));
            assertThat(stats.getSeqNoStats(), is(notNullValue()));

            assertThat(Strings.toString(stats.getSeqNoStats()),
                stats.getSeqNoStats().getMaxSeqNo(), equalTo(stats.getSeqNoStats().getGlobalCheckpoint()));
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
                case 0:
                    searchResponse = searchRequestBuilder.
                        setQuery(QueryBuilders.matchAllQuery()).get();
                    assertSearchResponseContainsAllIndexedDocs(searchResponse, docCount);
                    break;
                case 1:
                    int docIdToMatch = randomIntBetween(0, docCount - 1);
                    searchResponse = searchRequestBuilder.setQuery(QueryBuilders.termQuery("field", docIdToMatch)).get();
                    assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    SearchHit searchHit = searchResponse.getHits().getAt(0);

                    Map<String, Object> source = searchHit.getSourceAsMap();

                    assertThat(source, is(notNullValue()));
                    assertThat(source.get("field"), is(equalTo(docIdToMatch)));
                    assertThat(source.get("field2"), is(equalTo("Some text " + docIdToMatch)));
                    break;
                case 2:
                    searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchQuery("field2", "text")).get();
                    assertSearchResponseContainsAllIndexedDocs(searchResponse, docCount);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + testCase);
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

    private void assertPeerRecoveryWasSuccessful(RecoveryState recoveryState, String sourceNode, String targetNode) throws Exception {
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
            .filter(recoveryState -> recoveryState.getRecoverySource().equals(RecoverySource.PeerRecoverySource.INSTANCE))
            .filter(recoveryState -> recoveryState.getShardId().getId() == shardId)
            .collect(Collectors.toList());

        assertThat(peerRecoveries, is(not(empty())));
        return peerRecoveries.get(peerRecoveries.size() - 1);
    }

    private void updateSetting(String key, String value) {
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(Settings.builder().put(key, value));
        assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());
    }

    private void createRepo(String repoName, String type) {
        final Settings.Builder settings = Settings.builder()
            .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
            .put("location", randomRepoPath());
        createRepository(logger, repoName, type, settings, true);
    }
}
