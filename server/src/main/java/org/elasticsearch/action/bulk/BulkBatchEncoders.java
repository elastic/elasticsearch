/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingExtractor;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceBatchEncoder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-bulk helper that performs single-pass {@code XContent → ESCF} encoding while shard routing is
 * being computed, accumulating one row per item directly into the destination shard's row partition
 * inside an {@link EscfEncoder}. There is one encoder per concrete write index encountered in the
 * bulk; each encoder fans rows out to many partitions (one per destination shard).
 *
 * <p>Lifecycle: created at the start of a {@link BulkOperation#doRun() bulk run} only when
 * {@link #isBulkBatchEligible} returns true (i.e. every item in the bulk is structurally eligible
 * for batch encoding), used inside the initial-pass shard grouping, finalized via
 * {@link #finalizeBatches} just before per-shard {@code BulkShardRequest}s are constructed, and
 * {@link #close closed} when the bulk operation tears down.
 *
 * <p>Routing parity with the source-parser-based path is preserved by feeding the routing strategy's
 * {@link RoutingExtractor} (when one is available) data sourced from the encoder's parse pass.
 * Routing strategies without an extractor (Unpartitioned / Partitioned / IdAndRoutingOnly) fall
 * through to {@link IndexRouting#indexShard(IndexRequest)} since they don't need source parsing.
 * If the extractor throws (e.g. an array at a matched routing column), the helper catches it,
 * disables itself for the remainder of the bulk, and every item routes through the inline-source
 * path.
 *
 * <p>Bulk-wide all-or-nothing: the decision to use batch encoding is made once for the whole bulk by
 * the pre-scan in {@link BulkOperation#doRun()}. If a runtime encoder failure happens mid-grouping
 * — typically because the source bytes that already passed {@code BulkRequestParser} validation
 * fail the encoder's full parse — {@link #tryEncodeAndRoute} signals that via {@link #disabled()}
 * and the rest of the bulk goes through the inline-source path. {@link #finalizeBatches} returns an
 * empty map when disabled, so previously-committed rows are simply discarded and items keep their
 * inline source.
 */
final class BulkBatchEncoders implements Releasable {

    private static final Logger logger = LogManager.getLogger(BulkBatchEncoders.class);

    /** Sentinel returned from {@link #tryEncodeAndRoute} when the item cannot be batch-encoded. */
    static final int NOT_BATCHABLE = -1;

    private static final class IndexState {
        final SourceBatchEncoder encoder;
        final RoutingExtractor extractor;
        final Map<ShardId, List<PendingAttachment>> pendingByShard = new HashMap<>();

        IndexState(SourceBatchEncoder encoder, RoutingExtractor extractor) {
            this.encoder = encoder;
            this.extractor = extractor;
        }
    }

    private record PendingAttachment(IndexRequest indexRequest, int rowIndex) {}

    private final Map<Index, IndexState> indexStates = new HashMap<>();
    private boolean disabled;
    private boolean closed;

    /**
     * Returns true if every item in {@code bulkRequest} is structurally eligible to be batch-encoded:
     * an {@link IndexRequest} with inline source bytes, a known content type, and no pre-attached
     * batch row. If false, the bulk goes through the inline-source path end-to-end and no encoder
     * helper is created.
     */
    static boolean isBulkBatchEligible(BulkRequest bulkRequest) {
        if (bulkRequest.isSimulated()) {
            return false;
        }
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            if (request instanceof IndexRequest indexRequest) {
                if (isItemBatchEligible(indexRequest) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return bulkRequest.requests.isEmpty() == false;
    }

    /**
     * Per-item batch eligibility. Used by {@link #isBulkBatchEligible}; exposed for tests so the
     * pre-scan logic can be exercised in isolation.
     */
    static boolean isItemBatchEligible(IndexRequest request) {
        return request.indexSource().hasSource() && request.getContentType() != null && request.indexSource().hasSourceRow() == false;
    }

    /**
     * True after {@link #tryEncodeAndRoute} has hit a runtime encoder failure. Once disabled, the
     * helper still returns shard ids (so grouping can continue normally) but no batches are produced
     * by {@link #finalizeBatches} — every item ends up routed via the inline-source path.
     */
    boolean disabled() {
        return disabled;
    }

    /**
     * Routes {@code request}, encoding it into its destination shard's batch on the way. Falls back to
     * plain {@link DocWriteRequest#route} when this helper has been disabled by an earlier failure or
     * when the item turns out not to be batchable, so callers get a shard id either way.
     *
     * <p>The pre-scan in {@link BulkOperation#doRun()} guarantees every item is an {@link IndexRequest}
     * with inline source and a known content type, so eligibility is not re-checked per item.
     */
    int routeAndEncode(IndexRequest request, Index concreteIndex, IndexRouting indexRouting) {
        if (disabled) {
            return request.route(indexRouting);
        }
        int shardId = tryEncodeAndRoute(request, concreteIndex, indexRouting);
        return shardId == NOT_BATCHABLE ? request.route(indexRouting) : shardId;
    }

    /**
     * Encode {@code request} into the per-(concrete-index) encoder, compute its shard id (via the
     * routing strategy's extractor when applicable, falling back to
     * {@link IndexRouting#indexShard(IndexRequest)} otherwise), commit the staged row to the
     * destination shard's partition, and return the shard id.
     *
     * @return the destination shard id within {@code concreteIndex}, or {@link #NOT_BATCHABLE} if
     *         the encoder failed for this item — in which case the entire bulk's batch is
     *         abandoned ({@link #disabled()} becomes true) and the caller must route the item via
     *         {@link IndexRouting#indexShard(IndexRequest)} on the inline source.
     */
    int tryEncodeAndRoute(IndexRequest request, Index concreteIndex, IndexRouting indexRouting) {
        if (disabled) {
            return NOT_BATCHABLE;
        }
        IndexState state = indexStates.computeIfAbsent(
            concreteIndex,
            idx -> new IndexState(new EscfEncoder(), indexRouting.newRoutingExtractor())
        );
        if (state.extractor != null) {
            state.extractor.reset();
        }
        LeafSink sink = state.extractor != null ? state.extractor : LeafSink.NO_OP;
        XContentType contentType = request.getContentType();
        try {
            state.encoder.parseToScratch(request.indexSource().bytes(), contentType, sink);
        } catch (Exception e) {
            // Either the source bytes failed the encoder's parse (rare — they already passed
            // BulkRequestParser validation), or the extractor threw because it can't handle the
            // input (e.g. an array at a matched routing column). Either way, abandon the entire
            // bulk's batch: items already committed are discarded by finalizeBatches returning
            // empty, and subsequent items skip encoding (see the disabled check above). The
            // encoder's scratch will be reset at the start of the next parseToScratch call, so we
            // don't need to clean up here.
            logger.debug("batch encoding / routing extraction failed; abandoning batch for the rest of this bulk", e);
            disabled = true;
            return NOT_BATCHABLE;
        }
        int shardIdInt = state.extractor != null ? state.extractor.computeShardId(request) : indexRouting.indexShard(request);
        ShardId destShardId = new ShardId(concreteIndex, shardIdInt);
        try {
            int rowIndex = state.encoder.commitScratchTo(shardIdInt);
            state.pendingByShard.computeIfAbsent(destShardId, k -> new ArrayList<>()).add(new PendingAttachment(request, rowIndex));
        } catch (Exception e) {
            // commitScratchTo failure indicates internal-state corruption (IO error on the
            // underlying stream). Surface it; the per-item catch in groupRequestsByShards turns it
            // into a per-item failure response.
            throw new IllegalStateException("Failed to commit batch row for item to shard " + destShardId, e);
        }
        return shardIdInt;
    }

    /**
     * Build the batch for every shard that received committed rows, set the batch row reference
     * on each item routed there (replacing inline source bytes with a row reference), and return
     * the resulting batches keyed by ShardId. Returns an empty map when {@link #disabled()} is true.
     */
    Map<ShardId, SourceBatch> finalizeBatches() {
        if (disabled) {
            return Collections.emptyMap();
        }
        Map<ShardId, SourceBatch> batchesByShard = new HashMap<>();
        for (IndexState state : indexStates.values()) {
            for (Map.Entry<ShardId, List<PendingAttachment>> entry : state.pendingByShard.entrySet()) {
                List<PendingAttachment> pending = entry.getValue();
                if (pending.isEmpty()) {
                    continue;
                }
                ShardId shardId = entry.getKey();
                SourceBatch batch = state.encoder.buildPartition(shardId.getId());
                batchesByShard.put(shardId, batch);
                for (PendingAttachment attachment : pending) {
                    attachment.indexRequest.indexSource().setSourceRow(batch, attachment.rowIndex);
                }
            }
        }
        return batchesByShard;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (IndexState state : indexStates.values()) {
            state.encoder.close();
        }
        indexStates.clear();
    }
}
