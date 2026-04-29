/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.seqno;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertRetentionLeasesAdvanced;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsSeqNoDocValuesCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SeqNoPruningIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testSeqNoPrunedAfterMerge() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(prepareIndex(indexName).setSource("field", "value-" + batch + "-" + doc));
            }
            assertNoFailures(bulk.get());
        }

        flushAndRefresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        // waits for retention leases to advance past all docs
        assertRetentionLeasesAdvanced(client(), indexName, totalDocs);

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, 1);

        final boolean peerRecovery = randomBoolean();
        if (peerRecovery) {
            logger.info("--> triggering peer recovery by adding a replica");
            setReplicaCount(1, indexName);
            ensureGreen(indexName);
        } else {
            logger.info("--> triggering relocation via move allocation command");
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            var primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
            String sourceNode = primaryShard.currentNodeId();
            String sourceNodeName = state.nodes().get(sourceNode).getName();
            String targetNodeName = state.nodes()
                .getDataNodes()
                .values()
                .stream()
                .filter(n -> n.getName().equals(sourceNodeName) == false)
                .findFirst()
                .orElseThrow()
                .getName();

            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNodeName, targetNodeName));
            waitForRelocation();
        }

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, peerRecovery ? 2 : 1);
    }

    public void testSeqNoPartiallyPrunedWithRetentionLease() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(prepareIndex(indexName).setSource("field", "value-" + batch + "-" + doc));
            }
            assertNoFailures(bulk.get());
        }

        flushAndRefresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        // add a retention lease holding at a sequence number in the middle of the indexed range
        final long maxSeqNo = indicesAdmin().prepareStats(indexName).get().getShards()[0].getSeqNoStats().getMaxSeqNo();
        final long retentionLeaseSeqNo = randomLongBetween(1, maxSeqNo);
        final var retentionLeaseId = randomIdentifier();
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        client().execute(
            RetentionLeaseActions.ADD,
            new RetentionLeaseActions.AddRequest(shardId, retentionLeaseId, retentionLeaseSeqNo, "test")
        ).actionGet();

        // wait for peer recovery retention leases to advance past all docs; the custom lease stays
        assertBusy(() -> internalCluster().forEveryIndexShard(shardId.getIndex(), indexShard -> {
            for (RetentionLease lease : indexShard.getRetentionLeases().leases()) {
                if (lease.id().equals(retentionLeaseId)) {
                    assertThat(lease.retainingSequenceNumber(), equalTo(retentionLeaseSeqNo));
                } else {
                    assertThat(
                        "retention lease [" + lease.id() + "] should have advanced",
                        lease.retainingSequenceNumber(),
                        equalTo(maxSeqNo + 1)
                    );
                }
            }
        }));

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        // seq_no doc values should still be present because the custom retention lease prevented full pruning
        assertShardsHaveSeqNoDocValues(indexName, true, 1);

        // verify only docs with seq_no >= retentionLeaseSeqNo retained their doc values
        final long expectedRetainedDocs = maxSeqNo + 1 - retentionLeaseSeqNo;

        final var checkedShards = new AtomicInteger();
        internalCluster().forEveryIndexShard(shardId.getIndex(), indexShard -> {
            Long docsWithSeqNoOnShard = indexShard.withEngineOrNull(engine -> {
                if (engine == null) {
                    return null;
                }
                try (var searcher = engine.acquireSearcher("assert_seq_no_count")) {
                    long nbDocsWithSeqNo = 0;
                    for (var leaf : searcher.getLeafContexts()) {
                        NumericDocValues seqNoDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                        if (seqNoDV != null) {
                            while (seqNoDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                nbDocsWithSeqNo++;
                            }
                        }
                    }
                    return nbDocsWithSeqNo;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            if (docsWithSeqNoOnShard != null) {
                assertThat(
                    "docs with seq_no >= " + retentionLeaseSeqNo + " should retain doc values",
                    docsWithSeqNoOnShard,
                    equalTo(expectedRetainedDocs)
                );
                checkedShards.incrementAndGet();
            }
        });
        assertThat("expected to verify at least one shard", checkedShards.get(), equalTo(1));

        // remove the custom retention lease, index more data and force merge again to verify full pruning
        client().execute(RetentionLeaseActions.REMOVE, new RetentionLeaseActions.RemoveRequest(shardId, retentionLeaseId)).actionGet();

        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < docsPerBatch; doc++) {
            bulk.add(prepareIndex(indexName).setSource("field", "value-extra-" + doc));
        }
        assertNoFailures(bulk.get());

        flushAndRefresh(indexName);

        final long newMaxSeqNo = indicesAdmin().prepareStats(indexName).get().getShards()[0].getSeqNoStats().getMaxSeqNo();

        // wait for all retention leases to advance past all docs
        assertRetentionLeasesAdvanced(client(), indexName, newMaxSeqNo + 1);

        forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs + docsPerBatch);
        assertShardsHaveSeqNoDocValues(indexName, false, 1);
    }

    /**
     * Verifies that seq_no doc values are retained on the primary during an in-progress peer recovery.
     * The recovering replica's retention lease prevents the merge policy from pruning seq_no doc values for operations
     * that still need to be replayed to the replica.
     */
    public void testSeqNoRetainedDuringInProgressRecovery() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0)
                .build()
        );
        ensureGreen(indexName);

        final int docsPerBatch = randomIntBetween(20, 50);
        for (int batch = 0; batch < 2; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(prepareIndex(indexName).setSource("field", "value-first-" + batch + "-" + doc));
            }
            assertNoFailures(bulk.get());
            flush(indexName);
        }
        refresh(indexName);

        final int firstBatchTotalDocs = 2 * docsPerBatch;
        // Wait for leases to advance, then force merge so minRetainedSeqNo advances and first-batch seq_no values are pruned
        assertRetentionLeasesAdvanced(client(), indexName, firstBatchTotalDocs);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get().getFailedShards(), equalTo(0));

        refresh(indexName);

        assertShardsHaveSeqNoDocValues(indexName, false, 2);

        var clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        var primaryShard = clusterState.routingTable().index(indexName).shard(0).primaryShard();
        String primaryNodeName = clusterState.nodes().get(primaryShard.currentNodeId()).getName();
        String replicaNodeName = clusterState.routingTable()
            .index(indexName)
            .shard(0)
            .assignedShards()
            .stream()
            .filter(s -> s.primary() == false)
            .map(s -> clusterState.nodes().get(s.currentNodeId()).getName())
            .findFirst()
            .orElseThrow();

        final var replicaDataPathSettings = internalCluster().dataPathSettings(replicaNodeName);
        internalCluster().stopNode(replicaNodeName);

        // Index a third batch while the replica is down — the replica's retention lease is now stale
        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < docsPerBatch; doc++) {
            bulk.add(prepareIndex(indexName).setSource("field", "value-second-" + doc));
        }
        assertNoFailures(bulk.get());
        flush(indexName);

        final long totalDocs = firstBatchTotalDocs + docsPerBatch;
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);

        CountDownLatch recoveryBlocked = new CountDownLatch(1);
        CountDownLatch unblockRecovery = new CountDownLatch(1);
        AtomicInteger translogOpsCount = new AtomicInteger();
        MockTransportService primaryTransport = MockTransportService.getInstance(primaryNodeName);
        primaryTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.TRANSLOG_OPS.equals(action)) {
                if (translogOpsCount.incrementAndGet() > 1) {
                    recoveryBlocked.countDown();
                    safeAwait(unblockRecovery);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Use a tiny chunk size so translog ops are sent in many small batches,
        // allowing us to block recovery mid-way after some operations have already been replayed
        updateClusterSettings(Settings.builder().put(RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE.getKey(), ByteSizeValue.ofBytes(100)));

        try {
            internalCluster().startDataOnlyNode(replicaDataPathSettings);
            safeAwait(recoveryBlocked);
            assertThat("at least one translog ops batch should have been sent before blocking", translogOpsCount.get(), greaterThan(1));

            // Force merge on the primary — the retention lock held by the in-progress recovery prevents
            // minRetainedSeqNo from advancing, so the third batch's seq_no doc values are preserved
            var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
            assertThat(forceMerge.getFailedShards(), equalTo(0));

            refresh(indexName);

            // The primary should still have seq_no doc values for the third batch — the recovery's retention lock
            // prevents minRetainedSeqNo from advancing past the operations the replica still needs
            var primaryIndicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
            assertShardsSeqNoDocValuesCount(primaryIndicesService, indexName, docsPerBatch, 1);
        } finally {
            unblockRecovery.countDown();
            primaryTransport.clearAllRules();
            updateClusterSettings(Settings.builder().putNull(RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE.getKey()));
        }

        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);

        // Index one more doc and flush to guarantee every shard has at least 2 segments, so force merge actually triggers pruning
        indexDoc(indexName, "extra", "field", "value-extra");
        flush(indexName);

        final long finalTotalDocs = totalDocs + 1;
        assertRetentionLeasesAdvanced(client(), indexName, finalTotalDocs);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get().getFailedShards(), equalTo(0));

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), finalTotalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, 2);
    }

    public void testSeqNoPrunedAfterMergeWithTsdbCodec() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final var indexName = randomIdentifier();
        final Instant now = Instant.now();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now.minusSeconds(3600).toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now.plusSeconds(3600).toString())
                    .put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                    .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .build()
            ).setMapping("@timestamp", "type=date", "hostname", "type=keyword,time_series_dimension=true", "field", "type=keyword")
        );
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        long timestampMillis = now.toEpochMilli() - totalDocs;
        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(
                    prepareIndex(indexName).setSource(
                        "@timestamp",
                        timestampMillis++,
                        "hostname",
                        "host-" + batch + "-" + doc,
                        "field",
                        "value"
                    )
                );
            }
            assertNoFailures(bulk.get());
        }

        flushAndRefresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        assertRetentionLeasesAdvanced(client(), indexName, totalDocs);

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertNoFailures(forceMerge);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, 1);
    }

    /**
     * Verifies that index and delete operations succeed on replicas after _seq_no doc values have been pruned
     * by a force merge.
     */
    public void testWritesSucceedOnReplicaAfterSeqNoPruning() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);
        final int totalDocs = nbBatches * docsPerBatch;

        final Set<String> docsIds = new HashSet<>();
        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                var docId = "doc-" + (batch * docsPerBatch + doc);
                bulk.add(prepareIndex(indexName).setId(docId).setSource("field", "value-for-" + docId));
                docsIds.add(docId);
            }
            assertNoFailures(bulk.get());
        }

        flushAndRefresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 2);

        assertRetentionLeasesAdvanced(client(), indexName, totalDocs);

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        refresh(indexName);
        assertShardsHaveSeqNoDocValues(indexName, false, 2);

        var deletedIds = randomSubsetOf(randomIntBetween(5, Math.min(20, totalDocs)), docsIds);
        for (var docId : deletedIds) {
            var response = client().prepareDelete(indexName, docId).get();
            assertThat(response.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        }

        var updatedIds = randomSubsetOf(randomIntBetween(5, Math.min(20, totalDocs)), docsIds);
        for (var docId : updatedIds) {
            var response = prepareIndex(indexName).setId(docId).setSource("field", "updated").get();
            var expectedResult = deletedIds.contains(docId) ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED;
            assertThat(response.getResult(), equalTo(expectedResult));
        }

        ensureGreen(indexName);
    }
}
