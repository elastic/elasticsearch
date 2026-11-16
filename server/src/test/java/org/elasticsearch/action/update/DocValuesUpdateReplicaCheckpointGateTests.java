/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.DocValuesUpdateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

/**
 * The in-place doc-values fast path is only safe to replicate once the base document is on every copy that could receive the delta.
 * {@link UpdateHelper} gates it on the base sequence number being at or below every tracked copy's persisted local checkpoint (in-sync
 * copies and still-recovering ones alike); otherwise it falls back to a self-sufficient reindex that reconciles correctly under
 * out-of-order replication. These tests drive {@link UpdateHelper#prepare} against a real primary whose replica is held below the base,
 * for both an in-sync replica and a still-recovering one.
 */
public class DocValuesUpdateReplicaCheckpointGateTests extends IndexShardTestCase {

    private static final String MAPPING = """
        { "properties": { "count": { "type": "long", "index": false, "doc_values": { "updatable": true } } } }
        """;

    public void testInPlaceUpdateGatedOnReplicaCheckpoint() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final ShardId shardId = new ShardId("index", "_na_", 0);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            // updatable fields need sequence numbers, which columnar disables by default
            .put("index.disable_sequence_numbers", false)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings)
            .putMapping(MAPPING)
            .primaryTerm(0, 1)
            .build();

        final IndexShard primary = newShard(shardId, true, "primary-node", indexMetadata, null);
        IndexShard replica = null;
        try {
            recoverShardFromStore(primary);
            replica = newShard(shardId, false, "replica-node", indexMetadata, null);
            // Recover the (empty) replica so the primary tracks it as an in-sync copy that gates the global checkpoint.
            recoverReplica(replica, primary, true);

            final String replicaAllocationId = replica.routingEntry().allocationId().getId();
            // Hold the replica's acknowledged checkpoint below the write frontier so it is not yet known to hold the base.
            primary.updateLocalCheckpointForShard(replicaAllocationId, SequenceNumbers.NO_OPS_PERFORMED);

            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();
            assertThat(primary.getMinTrackedReplicaLocalCheckpoint(), lessThan(baseSeqNo));

            final UpdateHelper updateHelper = new UpdateHelper(mock(ScriptService.class));
            final UpdateRequest request = new UpdateRequest(shardId.getIndexName(), "1").doc(Map.of("count", 42));

            // Base above the in-sync replica's checkpoint: the delta is not safe to ship, so the update is realized as a full reindex.
            UpdateHelper.Result gated = updateHelper.prepare(
                request,
                primary,
                () -> 0L,
                FetchSourceContext.FETCH_SOURCE,
                SplitShardCountSummary.UNSET,
                true
            );
            assertThat(gated.action(), instanceOf(IndexRequest.class));

            // Release the replica's checkpoint up to the base: every in-sync replica now holds it, so the in-place fast path is taken.
            primary.updateLocalCheckpointForShard(replicaAllocationId, baseSeqNo);
            assertThat(primary.getMinTrackedReplicaLocalCheckpoint(), greaterThanOrEqualTo(baseSeqNo));

            UpdateHelper.Result inPlace = updateHelper.prepare(
                request,
                primary,
                () -> 0L,
                FetchSourceContext.FETCH_SOURCE,
                SplitShardCountSummary.UNSET,
                true
            );
            assertThat(inPlace.action(), instanceOf(DocValuesUpdateRequest.class));
        } finally {
            closeShards(primary, replica);
        }
    }

    /**
     * The gate must also account for still-recovering copies, not just in-sync ones. A recovering replica is already tracked and
     * receives live deltas concurrently with its recovery, so shipping it a delta whose base it has not caught up to would fail on the
     * copy. Here the primary tracks a recovering (tracked but not yet in-sync) replica that is held below the base: the in-sync
     * checkpoint ignores that copy entirely, but the tracked checkpoint does not, so the update is realized as a full reindex until the
     * copy catches up.
     */
    public void testInPlaceUpdateGatedOnRecoveringCopyCheckpoint() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final ShardId shardId = new ShardId("index", "_na_", 0);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            // updatable fields need sequence numbers, which columnar disables by default
            .put("index.disable_sequence_numbers", false)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings)
            .putMapping(MAPPING)
            .primaryTerm(0, 1)
            .build();

        final IndexShard primary = newShard(shardId, true, "primary-node", indexMetadata, null);
        try {
            recoverShardFromStore(primary);

            // Reach the mid-recovery state for a replica: the primary knows it as an initializing (not in-sync) copy and has initiated
            // tracking on it, so it is tracked and would receive live deltas, but it has not been marked in-sync yet. Building the state
            // directly avoids running a full recovery, whose finalization would mark the copy in-sync.
            final ShardRouting replicaRouting = TestShardRouting.newShardRouting(
                shardId,
                "replica-node",
                false,
                ShardRoutingState.INITIALIZING
            );
            final String replicaAllocationId = replicaRouting.allocationId().getId();
            final IndexShardRoutingTable routingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                .addShard(replicaRouting)
                .build();
            primary.updateShardState(
                primary.routingEntry(),
                primary.getPendingPrimaryTerm(),
                null,
                currentClusterStateVersion.incrementAndGet(),
                Set.of(primary.routingEntry().allocationId().getId()),
                routingTable
            );
            // A tracked copy must hold a peer-recovery retention lease (the tracker invariant enforces this); real recovery clones the
            // primary's lease for the target before initiating tracking.
            primary.cloneLocalPeerRecoveryRetentionLease("replica-node", ActionListener.noop());
            primary.initiateTracking(replicaAllocationId);

            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();

            // The recovering copy has not received the base, so it holds the tracked checkpoint below it. Gating on the in-sync copies
            // alone would miss this still-recovering copy, which is exactly why the fast path gates on the tracked checkpoint.
            assertThat(primary.getMinTrackedReplicaLocalCheckpoint(), lessThan(baseSeqNo));

            final RecordingMeterRegistry meter = new RecordingMeterRegistry();
            final UpdateHelper updateHelper = new UpdateHelper(mock(ScriptService.class), new DocValuesUpdateMetrics(meter));
            final UpdateRequest request = new UpdateRequest(shardId.getIndexName(), "1").doc(Map.of("count", 42));

            // Base above the recovering copy's checkpoint: the delta is not safe to ship, so the update is realized as a full reindex.
            UpdateHelper.Result gated = updateHelper.prepare(
                request,
                primary,
                () -> 0L,
                FetchSourceContext.FETCH_SOURCE,
                SplitShardCountSummary.UNSET,
                true
            );
            assertThat(gated.action(), instanceOf(IndexRequest.class));
            assertThat(counter(meter, DocValuesUpdateMetrics.REINDEX_FALLBACK_TOTAL), equalTo(1L));
            assertThat(counter(meter, DocValuesUpdateMetrics.IN_PLACE_TOTAL), equalTo(0L));

            // Once the recovering copy catches up to the base, the delta is safe to ship and the in-place fast path is taken.
            primary.updateLocalCheckpointForShard(replicaAllocationId, baseSeqNo);
            assertThat(primary.getMinTrackedReplicaLocalCheckpoint(), greaterThanOrEqualTo(baseSeqNo));

            UpdateHelper.Result inPlace = updateHelper.prepare(
                request,
                primary,
                () -> 0L,
                FetchSourceContext.FETCH_SOURCE,
                SplitShardCountSummary.UNSET,
                true
            );
            assertThat(inPlace.action(), instanceOf(DocValuesUpdateRequest.class));
            assertThat(counter(meter, DocValuesUpdateMetrics.IN_PLACE_TOTAL), equalTo(1L));
            assertThat(counter(meter, DocValuesUpdateMetrics.REINDEX_FALLBACK_TOTAL), equalTo(1L));
        } finally {
            closeShards(primary);
        }
    }

    /**
     * The gate also races while the write holds its primary operation permit. A {@code _bulk} update item evaluates the gate inside the
     * shard bulk action, under the primary permit. Peer recovery starts tracking a new copy through
     * {@code RecoverySourceHandler#runUnderPrimaryPermit}, which takes an ordinary, non-exclusive permit, so it does not wait for the
     * in-flight write. {@code ReplicationOperation} reads the replication group only after the primary has applied the operation, so a
     * delta whose gate passed before tracking started is still sent to the new copy. That copy has not received the base yet and fails
     * the delta with {@code DocValuesUpdateBaseNotPresentException} (see {@link DocValuesUpdateReplicaBaseNotPresentTests}).
     * <p>
     * The final assertion captures the current behaviour: the newly tracked copy does not yet hold the base. The real fix is a
     * replica-side wait-for-base, left as a follow-up; when it lands this flips to {@code greaterThanOrEqualTo(baseSeqNo)}.
     */
    public void testCopyTrackedWhileGatedWriteHoldsPrimaryPermitLacksBase() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final ShardId shardId = new ShardId("index", "_na_", 0);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            // updatable fields need sequence numbers, which columnar disables by default
            .put("index.disable_sequence_numbers", false)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings)
            .putMapping(MAPPING)
            .primaryTerm(0, 1)
            .build();

        final IndexShard primary = newShard(shardId, true, "primary-node", indexMetadata, null);
        try {
            recoverShardFromStore(primary);
            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();

            final UpdateHelper updateHelper = new UpdateHelper(mock(ScriptService.class));
            final UpdateRequest request = new UpdateRequest(shardId.getIndexName(), "1").doc(Map.of("count", 42));

            // The write takes its primary operation permit, as TransportShardBulkAction does before it prepares a _bulk update item.
            final PlainActionFuture<Releasable> writePermit = new PlainActionFuture<>();
            primary.acquirePrimaryOperationPermit(writePermit, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            try (Releasable ignored = safeGet(writePermit)) {
                // No other copy is tracked yet, so the gate passes and the update is realized as an in-place delta.
                UpdateHelper.Result inPlace = updateHelper.prepare(
                    request,
                    primary,
                    () -> 0L,
                    FetchSourceContext.FETCH_SOURCE,
                    SplitShardCountSummary.UNSET,
                    true
                );
                assertThat(inPlace.action(), instanceOf(DocValuesUpdateRequest.class));

                // Meanwhile, peer recovery of a new replica reaches initiateTracking.
                final ShardRouting replicaRouting = TestShardRouting.newShardRouting(
                    shardId,
                    "replica-node",
                    false,
                    ShardRoutingState.INITIALIZING
                );
                final String replicaAllocationId = replicaRouting.allocationId().getId();
                final IndexShardRoutingTable routingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                    .addShard(replicaRouting)
                    .build();
                primary.updateShardState(
                    primary.routingEntry(),
                    primary.getPendingPrimaryTerm(),
                    null,
                    currentClusterStateVersion.incrementAndGet(),
                    Set.of(primary.routingEntry().allocationId().getId()),
                    routingTable
                );
                primary.cloneLocalPeerRecoveryRetentionLease("replica-node", ActionListener.noop());

                // RecoverySourceHandler#runUnderPrimaryPermit takes an ordinary permit, which is granted while the write still holds its
                // own.
                final PlainActionFuture<Void> tracked = new PlainActionFuture<>();
                primary.acquirePrimaryOperationPermit(tracked.delegateFailureAndWrap((l, permit) -> {
                    try (permit) {
                        primary.initiateTracking(replicaAllocationId);
                    }
                    l.onResponse(null);
                }), EsExecutors.DIRECT_EXECUTOR_SERVICE);
                safeGet(tracked);

                // ReplicationOperation reads the replication group after the primary applied the write, so the new copy gets the delta...
                assertThat(primary.getReplicationGroup().getReplicationTargets(), hasItem(replicaRouting));
                // ...even though it does not hold the base yet (the known on-permit race; flips when the wait-for-base follow-up lands).
                assertThat(primary.getMinTrackedReplicaLocalCheckpoint(), lessThan(baseSeqNo));
            }
        } finally {
            closeShards(primary);
        }
    }

    private static long counter(RecordingMeterRegistry meter, String name) {
        return meter.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name).stream().mapToLong(Measurement::getLong).sum();
    }
}
