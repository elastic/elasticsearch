/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Coordinator-side helper that turns a map of {@code index name → whole-index SourceBatch}
 * (supplied by the bulk request producer) into a {@code Map<ShardId, SourceBatch>} suitable for
 * attaching to individual {@link BulkShardRequest}s.
 *
 * <p>The producer hands us one batch that covers every row destined for a given concrete index,
 * without knowing which shard each row will land on. The sharder records the coordinator's
 * authoritative shard routing decision for each item (via {@link #recordRouting}), then once all
 * items are routed it uses {@link EscfBatchScatterer} to scatter the whole-index batch into
 * per-shard sub-batches and re-points every item's {@link org.elasticsearch.action.index.IndexSource}
 * at its shard-local row index.
 *
 * <p>Routing constraints: the coordinator must be able to decide the shard without reading the
 * document source. Supported strategies are the {@code IdAndRoutingOnly} family (hash of id /
 * routing) and {@link IndexRouting.ExtractFromSource.ForIndexDimensions} when the {@code _tsid}
 * has already been pre-computed and set on the request. {@link #checkRoutable} hard-fails the
 * item (and thus the whole bulk) if the routing strategy would require source parsing.
 *
 * <p>Scattered batches use {@link BytesRefRecycler#NON_RECYCLING_INSTANCE}, matching
 * {@link BulkBatchEncoders}. The batches are plain-heap and reclaimed by GC; we deliberately do
 * not close them after handing them off because a locally-executed {@link BulkShardRequest} reads
 * the batch asynchronously.
 *
 * <p>TODO: threading a pooled recycler through {@code TransportBulkAction} → {@code BulkOperation}
 * requires ref-counting each per-shard batch against {@code BulkShardRequest} completion first.
 */
final class SourceBatchSharder implements Releasable {

    private static final class IndexState {
        final EscfBatch sourceBatch;
        /** Set from the first {@link #recordRouting} call; null until then. */
        @Nullable
        Index concreteIndex;
        int shardCount = -1;
        /**
         * selectors[rowIndex] = shardId (or {@code shardCount} = discard for dropped rows).
         * Filled with {@link Integer#MAX_VALUE} initially; shardCount is not known until
         * {@link #recordRouting} is first called, so the discard value is patched at that point.
         */
        final int[] selectors;
        final IndexRequest[] items;   // items[rowIndex] = IndexRequest
        int lastRow = -1;             // monotonicity guard

        IndexState(EscfBatch sourceBatch) {
            this.sourceBatch = sourceBatch;
            int docCount = sourceBatch.docCount();
            this.selectors = new int[docCount];
            this.items = new IndexRequest[docCount];
            Arrays.fill(selectors, Integer.MAX_VALUE); // discard sentinel before shardCount known
        }
    }

    private final Map<String, IndexState> indexStates;

    private SourceBatchSharder(Map<String, IndexState> indexStates) {
        this.indexStates = indexStates;
    }

    /**
     * Returns a new {@link SourceBatchSharder} when {@code bulkRequest} carries pre-built batches,
     * or {@code null} when it does not.
     */
    @Nullable
    static SourceBatchSharder create(BulkRequest bulkRequest) {
        Map<String, SourceBatch> batches = bulkRequest.getPreBuiltBatches();
        if (batches == null || batches.isEmpty()) {
            return null;
        }
        Map<String, IndexState> indexStates = new HashMap<>(batches.size());
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            SourceBatch sb = entry.getValue();
            if (sb instanceof EscfBatch escfBatch) {
                indexStates.put(entry.getKey(), new IndexState(escfBatch));
            } else {
                throw new IllegalArgumentException(
                    "pre-built batch for index [" + entry.getKey() + "] must be an EscfBatch but was [" + sb.getClass().getName() + "]"
                );
            }
        }
        return new SourceBatchSharder(indexStates);
    }

    /**
     * Verifies that the routing strategy for an item that belongs to a pre-built batch can be
     * resolved without reading the document source. Must be called before
     * {@link IndexRequest#route(IndexRouting)} for row-bearing items.
     *
     * <p>Fails hard for {@link IndexRouting.ExtractFromSource.ForRoutingPath} (which parses source
     * to hash routing fields) and for {@link IndexRouting.ExtractFromSource.ForIndexDimensions}
     * when no {@code _tsid} has been pre-computed on the request.
     */
    void checkRoutable(IndexRequest request, String concreteIndexName, IndexRouting routing) {
        if (indexStates.containsKey(concreteIndexName) == false) {
            return; // not a pre-built-batch item; nothing to check
        }
        if (routing instanceof IndexRouting.ExtractFromSource efs) {
            if (efs instanceof IndexRouting.ExtractFromSource.ForIndexDimensions && request.tsid() != null) {
                return; // pre-computed tsid; hashSource will short-circuit
            }
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndexName
                    + "] routes by extracting fields from _source, but this bulk supplies a pre-built source batch"
                    + " with no inline source; supply a pre-computed _tsid or use an index whose routing depends"
                    + " only on _id/_routing"
            );
        }
    }

    /**
     * Records the coordinator's authoritative shard routing for one item. Must be called after
     * {@code postRoutingProcess} has run for the item.
     *
     * @param request        the item's {@link IndexRequest}
     * @param concreteIndex  the resolved concrete index (stores the Index with UUID)
     * @param shardId        the shard id returned by routing
     * @param shardCount     the total number of shards for the index
     * @throws IllegalArgumentException if the item has no source-row reference, or if rows arrive
     *                                  in non-monotonically-increasing order
     */
    void recordRouting(IndexRequest request, Index concreteIndex, int shardId, int shardCount) {
        IndexState state = indexStates.get(concreteIndex.getName());
        if (state == null) {
            return; // not a pre-built-batch index
        }
        if (request.indexSource().hasSourceRow() == false) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + concreteIndex.getName()
                    + "] must carry a source-row reference when a pre-built batch is attached"
            );
        }
        int rowIndex = request.indexSource().rowIndex();
        int docCount = state.sourceBatch.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is out of range [0, "
                    + docCount
                    + ") for pre-built batch of index ["
                    + concreteIndex.getName()
                    + "]"
            );
        }
        if (rowIndex <= state.lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + state.lastRow
                    + " for index ["
                    + concreteIndex.getName()
                    + "]; rows must arrive in ascending order"
            );
        }
        if (state.shardCount == -1) {
            // First item for this index: lock in the shard count and patch the discard sentinel.
            state.shardCount = shardCount;
            state.concreteIndex = concreteIndex;
            Arrays.fill(state.selectors, shardCount); // discard = shardCount
        }
        state.selectors[rowIndex] = shardId;
        state.items[rowIndex] = request;
        state.lastRow = rowIndex;
    }

    /**
     * Scatters each whole-index batch into per-shard sub-batches, re-points every item's
     * {@link org.elasticsearch.action.index.IndexSource} at its shard-local row index, and returns
     * the resulting map of {@link ShardId} → {@link SourceBatch}.
     *
     * <p>Must be called exactly once, after all items have been routed via {@link #recordRouting}.
     * The returned batches are plain-heap (non-recycled) and must not be closed by the caller;
     * they are reclaimed by GC when no longer referenced.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        Map<ShardId, SourceBatch> result = new HashMap<>();
        for (IndexState state : indexStates.values()) {
            if (state.shardCount == -1) {
                // No items were routed to this index (all dropped by validation); skip.
                continue;
            }
            assert state.concreteIndex != null;
            int partitionCount = state.shardCount + 1; // +1 for the discard bucket
            EscfBatch[] parts;
            try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
                parts = scatterer.scatter(state.sourceBatch, state.selectors, partitionCount);
            }
            // Close the discard bucket immediately; dropped rows are not indexed.
            EscfBatch discardBucket = parts[state.shardCount];
            if (discardBucket != null) {
                discardBucket.close();
                parts[state.shardCount] = null;
            }

            // Walk rows in ascending order, re-pointing each item at its shard-local row.
            int[] nextRow = new int[state.shardCount];
            for (int row = 0; row < state.sourceBatch.docCount(); row++) {
                IndexRequest req = state.items[row];
                if (req == null) {
                    // Row was dropped (validation failure); it went to the discard bucket.
                    continue;
                }
                int shard = state.selectors[row];
                assert shard >= 0 && shard < state.shardCount : "unexpected shard selector " + shard;
                assert parts[shard] != null : "null shard partition for shard " + shard;
                XContentType contentType = req.indexSource().contentType();
                req.indexSource().setSourceRow(parts[shard], nextRow[shard]++, contentType);
                result.putIfAbsent(new ShardId(state.concreteIndex, shard), parts[shard]);
            }
        }
        return result;
    }

    @Override
    public void close() {
        // Nothing to release: the source batches are owned by the caller; the scattered sub-batches
        // are plain-heap and GC'd. The scatterer is already closed inside shardBatches().
    }
}
