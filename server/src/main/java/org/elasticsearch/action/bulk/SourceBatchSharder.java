/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scatters the single pre-built batch attached to a {@link BulkRequest} into the per-shard batches
 * that {@link BulkShardRequest}s carry: {@link #prepareRouting} per item before it is routed,
 * {@link #recordRoutedShard} once its shard is known, {@link #shardBatches} when all are routed.
 *
 * <p>Step-1 constraint: exactly one pre-built batch per bulk, and that batch must resolve to
 * exactly one concrete write index. TSDB data streams whose documents span two backing indices, and
 * bulks that supply multiple named batches, are rejected with a clear error message and will be
 * supported in a follow-up.
 *
 * <p>Items carry no source, so routing must be resolvable without one; {@link #prepareRouting}
 * fails hard otherwise. All rows must route successfully — if any item is dropped before
 * {@link #recordRoutedShard} is called for it, {@link #shardBatches} throws. Discard-bucket
 * support for dropped rows will be added in a follow-up.
 *
 * <p>Callers must not close the scattered batches: they are read asynchronously and, being backed
 * by {@link BytesRefRecycler#NON_RECYCLING_INSTANCE}, are GC-reclaimed.
 *
 * <p>TODO: pooled recycler requires ref-counting each per-shard batch against shard-request
 * completion.
 */
final class SourceBatchSharder implements Releasable {

    private final String batchName;
    private final EscfBatch source;
    /** row -> shardId; set by {@link #recordRoutedShard}, read by {@link #shardBatches}. */
    private final int[] partitionIds;
    /** The request holding each routed row. */
    private final IndexRequest[] items;

    /**
     * The concrete index bound by the first item. A second distinct concrete index is rejected
     * because one shard batch can only be scattered from one source batch.
     */
    @Nullable
    private Index boundIndex;

    private int shardCount;
    /**
     * True when the index routes on {@code _tsid}, so every item must carry a pre-computed one.
     */
    private boolean requiresPrecomputedTsid;
    private int lastRow = -1;
    private int routedCount;
    private boolean scattered;

    private SourceBatchSharder(String batchName, EscfBatch source) {
        this.batchName = batchName;
        this.source = source;
        this.partitionIds = new int[source.docCount()];
        this.items = new IndexRequest[source.docCount()];
    }

    /**
     * Returns a sharder for {@code bulkRequest}'s pre-built batches, or {@code null} if it has
     * none.
     *
     * @throws IllegalArgumentException if more than one batch was supplied (not yet supported)
     * @throws IllegalArgumentException if the single batch is not an {@link EscfBatch}
     */
    @Nullable
    static SourceBatchSharder create(BulkRequest bulkRequest) {
        Map<String, SourceBatch> batches = bulkRequest.getPreBuiltBatches();
        if (batches == null || batches.isEmpty()) {
            return null;
        }
        if (batches.size() > 1) {
            throw new IllegalArgumentException(
                "pre-built source batch bulk carries "
                    + batches.size()
                    + " batches, but at most one is supported in step 1; multi-batch support will be added in a follow-up"
            );
        }
        Map.Entry<String, SourceBatch> only = batches.entrySet().iterator().next();
        return new SourceBatchSharder(only.getKey(), requireEscfBatch(only.getKey(), only.getValue()));
    }

    private static EscfBatch requireEscfBatch(String batchName, SourceBatch batch) {
        if (batch instanceof EscfBatch escfBatch) {
            return escfBatch;
        }
        throw new IllegalArgumentException(
            "pre-built batch for index [" + batchName + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
        );
    }

    /**
     * Checks that {@code request} is an item of a bulk that carries pre-built batches, which every
     * item of such a bulk must be: it cannot be mixed with deletes, updates, or inline-source index
     * requests, since a shard batch's rows must line up 1:1 with the items of its shard request.
     *
     * @throws IllegalArgumentException if the item is not an {@link IndexRequest}
     */
    static IndexRequest requireBatchItem(DocWriteRequest<?> request) {
        if (request instanceof IndexRequest indexRequest) {
            return indexRequest;
        }
        throw new IllegalArgumentException(
            "["
                + request.opType()
                + "] operation on index ["
                + request.index()
                + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                + " request carrying a source-row reference"
        );
    }

    /**
     * Validates that this item can be routed and, on the first item reaching a given concrete
     * index, validates the routing strategy and binds the index. Must run before
     * {@link IndexRequest#route(IndexRouting)}, which would parse the empty inline source.
     *
     * @throws IllegalArgumentException if the item holds no row, if no batch was supplied for the
     *                                  name it targets, if the request carries inline source, if a
     *                                  second distinct concrete index is seen (not yet supported),
     *                                  or if its routing strategy needs {@code _source}
     */
    void prepareRouting(IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        if (batchName.equals(request.index()) == false) {
            if (request.indexSource().hasSourceRow()) {
                throw new IllegalArgumentException(
                    "item targeting index ["
                        + request.index()
                        + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                        + " batches must be keyed by the index name set on the requests whose rows they hold"
                );
            }
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries inline source, but this bulk supplies a pre-built batch; the two cannot be mixed"
            );
        }
        if (request.indexSource().hasSourceRow() == false) {
            throw new IllegalArgumentException(
                "item targeting index [" + request.index() + "] must carry a source-row reference when a pre-built batch is attached"
            );
        }
        if (boundIndex == null) {
            bind(concreteIndex, routing, project);
        } else if (boundIndex.equals(concreteIndex) == false) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + batchName
                    + "] resolved to concrete index ["
                    + concreteIndex.getName()
                    + "] in addition to ["
                    + boundIndex.getName()
                    + "]; batches spanning multiple concrete indices (e.g. TSDB data streams with"
                    + " multiple backing indices) are not yet supported and will be added in a follow-up"
            );
        }
        if (requiresPrecomputedTsid && request.tsid() == null) {
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndex.getName()
                    + "] routes on _tsid, but this item of a pre-built source batch has no inline source to extract"
                    + " the dimensions from; supply a pre-computed _tsid"
            );
        }
    }

    /** Validates the concrete index's routing strategy and binds it as the single target. */
    private void bind(Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        if (routing instanceof IndexRouting.ExtractFromSource) {
            if (routing instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
                throw new IllegalArgumentException(
                    "index ["
                        + concreteIndex.getName()
                        + "] routes by extracting fields from _source, but this bulk supplies a pre-built source batch"
                        + " with no inline source; supply a pre-computed _tsid or use an index whose routing depends"
                        + " only on _id/_routing"
                );
            }
            // hashSource short-circuits on a pre-computed tsid, which each item must therefore carry.
            requiresPrecomputedTsid = true;
        }
        shardCount = project.getIndexSafe(concreteIndex).getNumberOfShards();
        boundIndex = concreteIndex;
    }

    /**
     * Records one item's shard. Must run after {@code postRoutingProcess} so the tsid/hash side
     * effects (if any) are visible before the request is stored.
     *
     * @throws IllegalArgumentException if the row index is out of range or rows arrive out of order
     * @throws IllegalStateException    if the shard is outside this index's shard count
     */
    void recordRoutedShard(IndexRequest request, int shardId) {
        if (shardId < 0 || shardId >= shardCount) {
            throw new IllegalStateException(
                "shard [" + shardId + "] is outside the shard count [" + shardCount + "] of index [" + boundIndex.getName() + "]"
            );
        }
        int rowIndex = request.indexSource().rowIndex();
        int docCount = source.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + request.index() + "]"
            );
        }
        if (rowIndex <= lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + lastRow
                    + " of pre-built batch ["
                    + request.index()
                    + "]; rows must arrive in ascending order"
            );
        }
        partitionIds[rowIndex] = shardId;
        items[rowIndex] = request;
        lastRow = rowIndex;
        routedCount++;
    }

    /**
     * Scatters the batch per shard and re-points every recorded item at its shard-local row.
     * A batch whose rows all landed on a single shard skips the scatter entirely. Returned batches
     * must not be closed by the caller.
     *
     * <p>Throws if any rows were dropped before routing (0 &lt; routedCount &lt; docCount). Discard-
     * bucket support for partially-dropped batches will be added in a follow-up.
     *
     * <p>Empty on any call after the first: the failure-store redirect pass re-enters
     * {@code executeBulkRequestsByShard} and must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        if (routedCount == 0) {
            // Every item failed before routing; nothing to scatter.
            return Map.of();
        }
        if (routedCount != source.docCount()) {
            throw new IllegalStateException(
                "pre-built batch ["
                    + batchName
                    + "] had "
                    + source.docCount()
                    + " rows but only "
                    + routedCount
                    + " were routed; dropped rows in pre-built batches are not yet supported and will be added in a follow-up"
            );
        }
        // Passthrough fast path: single shard, no scatter needed.
        if (shardCount == 1) {
            return Map.of(new ShardId(boundIndex, 0), source);
        }
        return scatter();
    }

    private Map<ShardId, SourceBatch> scatter() {
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(source, partitionIds, shardCount);
        }
        // Build the result map and re-point each item at its shard-local row.
        Map<ShardId, SourceBatch> result = new HashMap<>();
        int[] nextRow = new int[shardCount];
        for (int row = 0; row < partitionIds.length; row++) {
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row;
            result.putIfAbsent(new ShardId(boundIndex, partition), part);
            items[row].indexSource().setSourceRow(part, nextRow[partition]++, items[row].indexSource().contentType());
        }
        return result;
    }

    /**
     * Verifies every shard request can carry its rows over the wire. The wire format has no row
     * numbers — {@link BulkShardBatch#attachBatchToItems} rebuilds them from item ordinal — so a
     * shard's items must map 1:1 and in order onto its batch's rows, and no item may keep a row
     * reference no batch backs (it would index an empty source). Runs for every bulk: losing the
     * batches entirely is one way this breaks.
     *
     * @throws IllegalStateException on any mismatch; too late for per-item recovery
     */
    static void validateBatchAlignment(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            List<BulkItemRequest> items = entry.getValue();
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            if (shardBatch == null) {
                for (BulkItemRequest item : items) {
                    if (item.request() instanceof IndexRequest indexRequest && indexRequest.indexSource().hasSourceRow()) {
                        throw new IllegalStateException(
                            "item ["
                                + item.id()
                                + "] of shard ["
                                + entry.getKey()
                                + "] holds a source-row reference but its shard request has no batch attached;"
                                + " it would be indexed with an empty source"
                        );
                    }
                }
            } else if (BulkShardBatch.rowsAlignWithItems(shardBatch, items) == false) {
                throw new IllegalStateException(
                    "batch for shard ["
                        + entry.getKey()
                        + "] does not align with its items (batch rows: "
                        + shardBatch.docCount()
                        + ", items: "
                        + items.size()
                        + "); this indicates a bug in the scatter logic"
                );
            }
        }
    }

    @Override
    public void close() {
        // Source batches are owned by the caller; scattered sub-batches are GC'd.
    }
}
