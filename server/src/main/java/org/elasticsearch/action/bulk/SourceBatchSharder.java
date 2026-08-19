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
import org.elasticsearch.cluster.metadata.DataStream;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the pre-built batches attached to a {@link BulkRequest} into a {@code Map<ShardId, SourceBatch>}
 * for attaching to {@link BulkShardRequest}s: {@link #prepareRouting} per item before it is routed,
 * {@link ShardTarget#recordRouting} once its shard is known, {@link #shardBatches} when all are routed.
 *
 * <p>Batches are keyed by the name the producer set on its {@link IndexRequest}s — a concrete index, an
 * alias, or a data stream — so one batch fans out over however many concrete indices routing resolves to
 * (a time series data stream picks a backing index per document, see
 * {@link DataStream#selectTimeSeriesWriteIndex}), each with its own shard count and routing strategy.
 * Each index gets a contiguous block of scatter partitions, so a row's partition is
 * {@code partitionBase + shardId} and one {@link EscfBatchScatterer#scatter} call per batch covers the
 * whole fan-out.
 *
 * <p>Routing must be resolvable without the document source, which these items do not carry;
 * {@link #prepareRouting} fails hard otherwise. Scattered batches use
 * {@link BytesRefRecycler#NON_RECYCLING_INSTANCE} and are GC-reclaimed; callers must not close them (a
 * locally-executed shard request reads them asynchronously).
 *
 * <p>TODO: pooled recycler requires ref-counting each per-shard batch against shard-request completion.
 */
final class SourceBatchSharder implements Releasable {

    /** Partition of a row that never reached routing; rewritten to the discard partition before scattering. */
    private static final int UNROUTED = -1;

    /**
     * One producer-supplied batch and the routing decisions recorded against its rows. Row numbers span the
     * whole batch whatever index they end up in, so the ascending-row invariant lives here, not per index.
     */
    private static final class BatchGroup {
        private final String name;
        private final EscfBatch source;
        /** partitionIds[rowIndex] = {@code target.partitionBase + shardId}, or {@link #UNROUTED}. */
        private final int[] partitionIds;
        private final IndexRequest[] items;
        private final Map<Index, ShardTarget> targets = new HashMap<>();
        /** Next free partition base; once all targets are bound this is also the discard partition id. */
        private int nextPartition = 0;
        private int lastRow = -1;

        BatchGroup(String name, EscfBatch source) {
            this.name = name;
            this.source = source;
            int docCount = source.docCount();
            this.partitionIds = new int[docCount];
            this.items = new IndexRequest[docCount];
            Arrays.fill(partitionIds, UNROUTED);
        }
    }

    /**
     * The rows of one batch destined for one concrete index, holding that index's block of the batch's
     * partition space. Bound once, by the first item of the batch that resolves to the index.
     */
    static final class ShardTarget {
        private final BatchGroup group;
        private final Index index;
        private final int shardCount;
        private final int partitionBase;
        /** True when the index routes on {@code _tsid}, so every item must carry a pre-computed one. */
        private final boolean requiresPrecomputedTsid;

        private ShardTarget(BatchGroup group, Index index, int shardCount, int partitionBase, boolean requiresPrecomputedTsid) {
            this.group = group;
            this.index = index;
            this.shardCount = shardCount;
            this.partitionBase = partitionBase;
            this.requiresPrecomputedTsid = requiresPrecomputedTsid;
        }

        /**
         * Records the shard routing decision for one item. Must run after {@code postRoutingProcess} so the
         * tsid/hash side effects (if any) are visible before the request is stored.
         *
         * @throws IllegalArgumentException if the row index is out of range, or rows arrive out of order
         * @throws IllegalStateException if the shard is outside this index's shard count, which would spill
         *                               into the next target's block of the partition space
         */
        void recordRouting(IndexRequest request, int shardId) {
            if (shardId < 0 || shardId >= shardCount) {
                throw new IllegalStateException(
                    "shard [" + shardId + "] is outside the shard count [" + shardCount + "] of index [" + index.getName() + "]"
                );
            }
            int rowIndex = request.indexSource().rowIndex();
            int docCount = group.source.docCount();
            if (rowIndex < 0 || rowIndex >= docCount) {
                throw new IllegalArgumentException(
                    "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + group.name + "]"
                );
            }
            if (rowIndex <= group.lastRow) {
                throw new IllegalArgumentException(
                    "rowIndex "
                        + rowIndex
                        + " is not strictly greater than the previous row "
                        + group.lastRow
                        + " of pre-built batch ["
                        + group.name
                        + "]; rows must arrive in ascending order"
                );
            }
            group.partitionIds[rowIndex] = partitionBase + shardId;
            group.items[rowIndex] = request;
            group.lastRow = rowIndex;
        }
    }

    private final Map<String, BatchGroup> groups;
    /** Guards against two batches feeding one concrete index, which no single shard batch could represent. */
    private final Map<Index, BatchGroup> indexOwners = new HashMap<>();
    private boolean scattered;

    private SourceBatchSharder(Map<String, BatchGroup> groups) {
        this.groups = groups;
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
        Map<String, BatchGroup> groups = new HashMap<>(batches.size());
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            SourceBatch sb = entry.getValue();
            if (sb instanceof EscfBatch escfBatch) {
                groups.put(entry.getKey(), new BatchGroup(entry.getKey(), escfBatch));
            } else {
                throw new IllegalArgumentException(
                    "pre-built batch for index [" + entry.getKey() + "] must be an EscfBatch but was [" + sb.getClass().getName() + "]"
                );
            }
        }
        return new SourceBatchSharder(groups);
    }

    /**
     * Binds an item to the batch supplied for the name it targets and to the concrete index it resolved to,
     * returning the handle {@link ShardTarget#recordRouting} needs, or {@code null} when the item has no
     * pre-built batch. Must run after {@code preRoutingProcess} and before
     * {@link IndexRequest#route(IndexRouting)}, which would parse the empty inline source: the first item
     * to reach a concrete index validates its routing strategy and reserves its partitions.
     *
     * @throws IllegalArgumentException if the item and the batch map disagree on whether it holds a row,
     *                                  if the index is already fed by another batch, or if its routing
     *                                  cannot be resolved without the document source
     */
    @Nullable
    ShardTarget prepareRouting(IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        BatchGroup group = groups.get(request.index());
        if (group == null) {
            if (request.indexSource().hasSourceRow()) {
                throw new IllegalArgumentException(
                    "item targeting index ["
                        + request.index()
                        + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                        + " batches must be keyed by the index name set on the requests whose rows they hold"
                );
            }
            return null; // ordinary item with inline source
        }
        if (request.indexSource().hasSourceRow() == false) {
            throw new IllegalArgumentException(
                "item targeting index [" + request.index() + "] must carry a source-row reference when a pre-built batch is attached"
            );
        }
        ShardTarget target = group.targets.get(concreteIndex);
        if (target == null) {
            target = bind(group, concreteIndex, routing, project);
        }
        if (target.requiresPrecomputedTsid && request.tsid() == null) {
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndex.getName()
                    + "] routes on _tsid, but this item of a pre-built source batch has no inline source to extract"
                    + " the dimensions from; supply a pre-computed _tsid"
            );
        }
        return target;
    }

    /** Validates the index's routing strategy and reserves its block of the partition space. */
    private ShardTarget bind(BatchGroup group, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        BatchGroup owner = indexOwners.get(concreteIndex);
        if (owner != null && owner != group) {
            // A shard batch is scattered from exactly one source batch.
            // TODO: supporting fan-in would need an EscfBatch concat primitive.
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndex.getName()
                    + "] receives rows from pre-built batches ["
                    + owner.name
                    + "] and ["
                    + group.name
                    + "]; an index may only be fed by one batch"
            );
        }
        boolean requiresPrecomputedTsid = false;
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
        int shardCount = project.getIndexSafe(concreteIndex).getNumberOfShards();
        ShardTarget target = new ShardTarget(group, concreteIndex, shardCount, group.nextPartition, requiresPrecomputedTsid);
        group.nextPartition += shardCount;
        group.targets.put(concreteIndex, target);
        indexOwners.put(concreteIndex, group);
        return target;
    }

    /**
     * Scatters each batch into per-(concrete index, shard) sub-batches and re-points every recorded item's
     * {@link org.elasticsearch.action.index.IndexSource} at its shard-local row. Rows that never reached
     * routing — items dropped by validation — go to a discard bucket, closed here. Returned batches must
     * not be closed by the caller.
     *
     * <p>Returns an empty map on any call after the first: the failure-store redirect pass re-enters
     * {@code executeBulkRequestsByShard} with items that carry inline source, and must not re-scatter
     * batches whose shard requests are already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        Map<ShardId, SourceBatch> result = new HashMap<>();
        for (BatchGroup group : groups.values()) {
            if (group.nextPartition == 0) {
                // No item of this batch reached routing (all dropped by earlier validation); nothing to scatter.
                continue;
            }
            scatterGroup(group, result);
        }
        return result;
    }

    private static void scatterGroup(BatchGroup group, Map<ShardId, SourceBatch> result) {
        final int discardPartition = group.nextPartition;
        final int[] partitionIds = group.partitionIds;
        for (int row = 0; row < partitionIds.length; row++) {
            if (partitionIds[row] == UNROUTED) {
                partitionIds[row] = discardPartition;
            }
        }
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(group.source, partitionIds, discardPartition + 1);
        }
        // Close the discard bucket immediately; dropped rows are not indexed.
        EscfBatch discardBucket = parts[discardPartition];
        if (discardBucket != null) {
            discardBucket.close();
            parts[discardPartition] = null;
        }

        // Walk the partition space rather than the rows: a partition is non-null iff it received a row.
        for (ShardTarget target : group.targets.values()) {
            for (int shard = 0; shard < target.shardCount; shard++) {
                EscfBatch part = parts[target.partitionBase + shard];
                if (part != null) {
                    ShardId shardId = new ShardId(target.index, shard);
                    SourceBatch previous = result.put(shardId, part);
                    if (previous != null) {
                        throw new IllegalStateException("shard [" + shardId + "] was assigned two source batches");
                    }
                }
            }
        }

        // Re-point each item at its shard-local row: ascending row order matches the order the scatterer
        // appended rows within a partition.
        int[] nextRow = new int[discardPartition];
        for (int row = 0; row < partitionIds.length; row++) {
            IndexRequest item = group.items[row];
            if (item == null) {
                // Row was dropped before routing; it went to the discard bucket.
                continue;
            }
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row + " of batch [" + group.name + "]";
            item.indexSource().setSourceRow(part, nextRow[partition]++, item.indexSource().contentType());
        }
    }

    /**
     * Verifies every shard request can carry its rows over the wire. The wire format has no row numbers —
     * {@link BulkShardBatch#attachBatchToItems} reconstructs them from item ordinal — so a shard's items
     * must map 1:1 and in order onto its batch's rows, and no item may keep a row reference that no batch
     * backs (it would be indexed with an empty source). Runs for every bulk, including those with no
     * pre-built batches: losing the batches entirely is one of the ways this invariant breaks.
     *
     * @throws IllegalStateException on any mismatch; the batches are scattered and the items re-pointed by
     *                               now, so there is no per-item recovery
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
