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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scatters the pre-built batches attached to a {@link BulkRequest} into the per-shard batches that
 * {@link BulkShardRequest}s carry: {@link #prepareRouting} per item before it is routed,
 * {@link ConcreteIndexTarget#recordRoutedShard} once its shard is known, {@link #shardBatches} when all are routed.
 *
 * <p>A batch is keyed by the name its items target — index, alias, or data stream — so it fans out over
 * however many concrete indices routing resolves to, each with its own shard count and routing strategy
 * (a time series data stream picks a backing index per document). Each index gets a contiguous block of
 * partitions, so a row's partition is {@code partitionBase + shardId} and one
 * {@link EscfBatchScatterer#scatter} call per batch covers the whole fan-out.
 *
 * <p>Items carry no source, so routing must be resolvable without one; {@link #prepareRouting} fails hard
 * otherwise. Callers must not close the scattered batches: they are read asynchronously and, being backed
 * by {@link BytesRefRecycler#NON_RECYCLING_INSTANCE}, are GC-reclaimed.
 *
 * <p>TODO: pooled recycler requires ref-counting each per-shard batch against shard-request completion.
 */
final class SourceBatchSharder implements Releasable {

    /** A row that never reached routing; becomes the discard partition at scatter time. */
    private static final int UNROUTED = -1;

    /**
     * One pre-built batch and the routing decisions recorded against its rows. Row numbers span the whole
     * batch whatever index they end up in, so ordering is tracked here rather than per index.
     */
    private static final class BatchGroup {

        private final EscfBatch source;
        /** row -> {@code partitionBase + shardId}, or {@link #UNROUTED}. */
        private final int[] partitionIds;
        /** The request holding each routed row; null for rows that never reached routing. */
        private final IndexRequest[] items;
        /** One per concrete index the rows fanned out to, ordered by partition block. */
        private final List<ConcreteIndexTarget> targets = new ArrayList<>();
        /** Next free partition base; also the discard partition once every target is bound. */
        private int nextPartition = 0;
        private int lastRow = -1;

        BatchGroup(EscfBatch source) {
            this.source = source;
            this.partitionIds = new int[source.docCount()];
            this.items = new IndexRequest[source.docCount()];
            Arrays.fill(partitionIds, UNROUTED);
        }

        @Nullable
        ConcreteIndexTarget targetFor(Index concreteIndex) {
            for (ConcreteIndexTarget target : targets) {
                if (target.index.equals(concreteIndex)) {
                    return target;
                }
            }
            return null;
        }
    }

    /** One batch's rows for one concrete index, holding that index's block of the partition space. */
    static final class ConcreteIndexTarget {
        private final BatchGroup group;
        private final Index index;
        private final int shardCount;
        private final int partitionBase;
        /** True when the index routes on {@code _tsid}, so every item must carry a pre-computed one. */
        private final boolean requiresPrecomputedTsid;

        private ConcreteIndexTarget(BatchGroup group, Index index, int shardCount, int partitionBase, boolean requiresPrecomputedTsid) {
            this.group = group;
            this.index = index;
            this.shardCount = shardCount;
            this.partitionBase = partitionBase;
            this.requiresPrecomputedTsid = requiresPrecomputedTsid;
        }

        /**
         * Records one item's shard. Must run after {@code postRoutingProcess} so the tsid/hash side effects
         * are visible before the request is stored.
         *
         * @throws IllegalArgumentException if the row index is out of range or rows arrive out of order
         * @throws IllegalStateException if the shard is outside this index's shard count, which would spill
         *                               into the next target's partitions
         */
        void recordRoutedShard(IndexRequest request, int shardId) {
            if (shardId < 0 || shardId >= shardCount) {
                throw new IllegalStateException(
                    "shard [" + shardId + "] is outside the shard count [" + shardCount + "] of index [" + index.getName() + "]"
                );
            }
            int rowIndex = request.indexSource().rowIndex();
            int docCount = group.source.docCount();
            if (rowIndex < 0 || rowIndex >= docCount) {
                throw new IllegalArgumentException(
                    "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + request.index() + "]"
                );
            }
            if (rowIndex <= group.lastRow) {
                throw new IllegalArgumentException(
                    "rowIndex "
                        + rowIndex
                        + " is not strictly greater than the previous row "
                        + group.lastRow
                        + " of pre-built batch ["
                        + request.index()
                        + "]; rows must arrive in ascending order"
                );
            }
            group.partitionIds[rowIndex] = partitionBase + shardId;
            group.items[rowIndex] = request;
            group.lastRow = rowIndex;
        }
    }

    private final Map<String, BatchGroup> groups;
    /** Index -> the batch key feeding it; two batches for one index is a shape no shard batch can carry. */
    private final Map<Index, String> indexOwners = new HashMap<>();
    private boolean scattered;

    private SourceBatchSharder(Map<String, BatchGroup> groups) {
        this.groups = groups;
    }

    /** Returns a sharder for {@code bulkRequest}'s pre-built batches, or {@code null} if it has none. */
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
                groups.put(entry.getKey(), new BatchGroup(escfBatch));
            } else {
                throw new IllegalArgumentException(
                    "pre-built batch for index [" + entry.getKey() + "] must be an EscfBatch but was [" + sb.getClass().getName() + "]"
                );
            }
        }
        return new SourceBatchSharder(groups);
    }

    /**
     * Checks that {@code request} is an item of a bulk that carries pre-built batches, which every item of
     * such a bulk must be: it cannot be mixed with deletes, updates, or inline-source index requests, since
     * a shard batch's rows must line up 1:1 with the items of its shard request.
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
     * Binds an item to its batch and to the concrete index it resolved to, returning the handle
     * {@link ConcreteIndexTarget#recordRoutedShard} needs. Must run before {@link IndexRequest#route(IndexRouting)},
     * which would parse the empty inline source: the first item to reach an index validates its routing
     * strategy and reserves its partitions.
     *
     * @throws IllegalArgumentException if the item holds no row or no batch was supplied for the name it
     *                                  targets, if the index is already fed by another batch, or if its
     *                                  routing needs _source
     */
    ConcreteIndexTarget prepareRouting(IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
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
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries inline source, but this bulk supplies pre-built batches; the two cannot be mixed"
            );
        }
        if (request.indexSource().hasSourceRow() == false) {
            throw new IllegalArgumentException(
                "item targeting index [" + request.index() + "] must carry a source-row reference when a pre-built batch is attached"
            );
        }
        ConcreteIndexTarget target = group.targetFor(concreteIndex);
        if (target == null) {
            target = bind(group, request.index(), concreteIndex, routing, project);
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
    private ConcreteIndexTarget bind(
        BatchGroup group,
        String batchName,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        String owner = indexOwners.get(concreteIndex);
        if (owner != null && owner.equals(batchName) == false) {
            // A shard batch is scattered from exactly one source batch.
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndex.getName()
                    + "] receives rows from pre-built batches ["
                    + owner
                    + "] and ["
                    + batchName
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
        ConcreteIndexTarget target = new ConcreteIndexTarget(
            group,
            concreteIndex,
            shardCount,
            group.nextPartition,
            requiresPrecomputedTsid
        );
        group.nextPartition += shardCount;
        group.targets.add(target);
        indexOwners.put(concreteIndex, batchName);
        return target;
    }

    /**
     * Scatters each batch per (concrete index, shard) and re-points every recorded item at its shard-local
     * row. Unrouted rows — items dropped by validation — go to a discard bucket, closed here. Returned
     * batches must not be closed by the caller.
     *
     * <p>Empty on any call after the first: the failure-store redirect pass re-enters
     * {@code executeBulkRequestsByShard} and must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        Map<ShardId, SourceBatch> result = new HashMap<>();
        for (BatchGroup group : groups.values()) {
            if (group.nextPartition == 0) {
                // No item of this batch reached routing; nothing to scatter.
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

        // A partition is non-null iff it received a row, so walk the partition space, not the rows.
        for (ConcreteIndexTarget target : group.targets) {
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

        // Ascending row order matches the order the scatterer appended rows within a partition.
        int[] nextRow = new int[discardPartition];
        for (int row = 0; row < partitionIds.length; row++) {
            IndexRequest item = group.items[row];
            if (item == null) {
                // Dropped before routing; went to the discard bucket.
                continue;
            }
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row;
            item.indexSource().setSourceRow(part, nextRow[partition]++, item.indexSource().contentType());
        }
    }

    /**
     * Verifies every shard request can carry its rows over the wire. The wire format has no row numbers —
     * {@link BulkShardBatch#attachBatchToItems} rebuilds them from item ordinal — so a shard's items must
     * map 1:1 and in order onto its batch's rows, and no item may keep a row reference no batch backs (it
     * would index an empty source). Runs for every bulk: losing the batches entirely is one way this breaks.
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
