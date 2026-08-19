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
 * Turns a map of {@code index name → whole-index EscfBatch} into a {@code Map<ShardId, SourceBatch>}
 * for attaching to {@link BulkShardRequest}s. Call {@link #recordRouting} for each item as routing
 * is decided, then {@link #shardBatches} once all items are routed.
 *
 * <p>Routing must be resolvable without reading document source: id/routing hash strategies always
 * work; {@link IndexRouting.ExtractFromSource.ForIndexDimensions} works only when {@code _tsid} is
 * pre-computed. {@link #checkRoutable} fails hard otherwise.
 *
 * <p>Scattered batches use {@link BytesRefRecycler#NON_RECYCLING_INSTANCE} and are GC-reclaimed;
 * callers must not close them (a locally-executed shard request reads them asynchronously).
 *
 * <p>TODO: pooled recycler requires ref-counting each per-shard batch against shard-request completion.
 */
final class SourceBatchSharder implements Releasable {

    private static final class IndexState {
        final EscfBatch sourceBatch;
        /** Null until the first {@link #recordRouting} call. */
        @Nullable
        Index concreteIndex;
        int shardCount = -1;
        /**
         * selectors[rowIndex] = shardId; {@code shardCount} means discard. Pre-filled with
         * {@link Integer#MAX_VALUE}; patched to {@code shardCount} on the first {@link #recordRouting}.
         */
        final int[] selectors;
        final IndexRequest[] items;
        int lastRow = -1;

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
     * Verifies routing can be resolved without reading document source. Must be called before
     * {@link IndexRequest#route(IndexRouting)} for row-bearing items. Fails hard for
     * {@link IndexRouting.ExtractFromSource} strategies unless {@code _tsid} is pre-computed.
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
     * Records the shard routing decision for one item. Must be called after {@code postRoutingProcess}.
     *
     * @throws IllegalArgumentException if the item has no source-row reference, or rows arrive out of order
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
     * Scatters whole-index batches into per-shard sub-batches and re-points each item's
     * {@link org.elasticsearch.action.index.IndexSource} at its shard-local row index.
     * Must be called exactly once after all items are routed. Returned batches must not be closed.
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
        // Source batches are owned by the caller; scattered sub-batches are GC'd.
    }
}
