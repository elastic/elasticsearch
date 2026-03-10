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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SeqNoPruningIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    public void testSeqNoPrunedAfterMerge() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

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
        assertRetentionLeasesAdvanced(indexName, totalDocs);

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
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

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
        assertBusy(() -> {
            for (var indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                for (var indexService : indicesServices) {
                    if (indexService.index().getName().equals(indexName)) {
                        for (var indexShard : indexService) {
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
                        }
                    }
                }
            }
        });

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

        int checkedShards = 0;
        for (var indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (var indexService : indicesServices) {
                if (indexService.index().getName().equals(indexName)) {
                    for (var indexShard : indexService) {
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
                            checkedShards++;
                        }
                    }
                }
            }
        }
        assertThat("expected to verify at least one shard", checkedShards, equalTo(1));

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
        assertRetentionLeasesAdvanced(indexName, newMaxSeqNo + 1);

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

    public void testSeqNoPrunedAfterMergeWithTsdbCodecFails() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

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

        assertRetentionLeasesAdvanced(indexName, totalDocs);

        final var capturedUnassignedInfo = new AtomicReference<UnassignedInfo>();
        var shardFailureListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            var indexRoutingTable = state.routingTable().index(indexName);
            if (indexRoutingTable != null) {
                var primary = indexRoutingTable.shard(0).primaryShard();
                if (primary.state() == ShardRoutingState.UNASSIGNED
                    && primary.unassignedInfo() != null
                    && primary.unassignedInfo().reason() == UnassignedInfo.Reason.ALLOCATION_FAILED) {
                    capturedUnassignedInfo.set(primary.unassignedInfo());
                    return true;
                }
            }
            return false;
        });

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat("Expect a merge failure", forceMerge.getFailedShards(), equalTo(1));

        safeAwait(shardFailureListener);

        /**
         * [2026-03-10T06:35:15,636][WARN ][o.e.i.e.Engine           ][node_s1][generic][T#4] [jvsnptixp][0] failed engine [merge failed]
         * org.apache.lucene.index.MergePolicy$MergeException: java.lang.IllegalStateException: Wrong number of values added, expected: 1, got: 0
         * 	at org.elasticsearch.index.engine.InternalEngine$5.doRun(InternalEngine.java:3119) ~[main/:?]
         * 	at org.elasticsearch.common.util.concurrent.ThreadContext$ContextPreservingAbstractRunnable.doRun(ThreadContext.java:1114) ~[main/:?]
         * 	at org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:27) ~[main/:?]
         * 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1090) ~[?:?]
         * 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:614) ~[?:?]
         * 	at java.lang.Thread.run(Thread.java:1474) ~[?:?]
         * Caused by: java.lang.IllegalStateException: Wrong number of values added, expected: 1, got: 0
         * 	at org.apache.lucene.util.packed.DirectMonotonicWriter.finish(DirectMonotonicWriter.java:139) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesConsumer.writeField(ES819TSDBDocValuesConsumer.java:284) ~[main/:?]
         * 	at org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesConsumer.addNumericField(ES819TSDBDocValuesConsumer.java:161) ~[main/:?]
         * 	at org.elasticsearch.index.codec.tsdb.es819.XDocValuesConsumer.mergeNumericField(XDocValuesConsumer.java:40) ~[main/:?]
         * 	at org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesConsumer.mergeNumericField(ES819TSDBDocValuesConsumer.java:330) ~[main/:?]
         * 	at org.apache.lucene.codecs.DocValuesConsumer.merge(DocValuesConsumer.java:143) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat$FieldsWriter.merge(XPerFieldDocValuesFormat.java:130) ~[main/:?]
         * 	at org.apache.lucene.index.SegmentMerger.mergeDocValues(SegmentMerger.java:188) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.apache.lucene.index.SegmentMerger.mergeWithLogging(SegmentMerger.java:314) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.apache.lucene.index.SegmentMerger.merge(SegmentMerger.java:149) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.apache.lucene.index.IndexWriter.mergeMiddle(IndexWriter.java:5323) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.apache.lucene.index.IndexWriter.merge(IndexWriter.java:4784) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.apache.lucene.index.IndexWriter$IndexWriterMergeSource.merge(IndexWriter.java:6607) ~[lucene-core-10.4.0.jar:10.4.0 9983b7ce7fdd04f4d357688fb85c14277c15ea8d - 2026-02-20 18:05:31]
         * 	at org.elasticsearch.index.engine.ThreadPoolMergeScheduler.doMerge(ThreadPoolMergeScheduler.java:479) ~[main/:?]
         * 	at org.elasticsearch.index.engine.ThreadPoolMergeScheduler$MergeTask.run(ThreadPoolMergeScheduler.java:598) ~[main/:?]
         * 	at org.elasticsearch.index.engine.ThreadPoolMergeExecutorService.runMergeTask(ThreadPoolMergeExecutorService.java:370) ~[main/:?]
         * 	at org.elasticsearch.index.engine.ThreadPoolMergeExecutorService.lambda$enqueueMergeTaskExecution$11(ThreadPoolMergeExecutorService.java:338) ~[main/:?]
         * 	at org.elasticsearch.common.util.concurrent.ThreadContext$ContextPreservingRunnable.run(ThreadContext.java:1047) ~[main/:?]
         * 	... 3 more
         */

        var unassignedInfo = capturedUnassignedInfo.get();
        assertThat(unassignedInfo, notNullValue());
        assertThat(unassignedInfo.reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        var mergeFailure = ExceptionsHelper.unwrap(unassignedInfo.failure(), IllegalStateException.class);
        assertThat(mergeFailure, notNullValue());
        assertThat(mergeFailure.getMessage(), containsString("Wrong number of values added"));
    }

    /**
     * Waits for all retention leases on all copies of the given index to have their retaining sequence number
     * equal to the expected value.
     */
    private static void assertRetentionLeasesAdvanced(String indexName, long expectedRetainingSeqNo) throws Exception {
        assertBusy(() -> {
            for (var indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                for (var indexService : indicesServices) {
                    if (indexService.index().getName().equals(indexName)) {
                        for (var indexShard : indexService) {
                            for (RetentionLease lease : indexShard.getRetentionLeases().leases()) {
                                assertThat(
                                    "retention lease [" + lease.id() + "] should have advanced",
                                    lease.retainingSequenceNumber(),
                                    equalTo(expectedRetainingSeqNo)
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}
