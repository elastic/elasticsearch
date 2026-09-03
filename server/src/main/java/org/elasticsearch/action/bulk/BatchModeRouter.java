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
import org.elasticsearch.cluster.metadata.IndexAbstraction;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Per-bulk router: decides each item's destination shard and builds the per-shard {@link SourceBatch}.
 *
 * <p>Two modes, selected in {@link #create}:
 * <ul>
 *   <li><b>Provided batch</b> — caller supplied a pre-built {@link EscfBatch}; items carry source-row
 *       references. Shard assignment is deferred: all rows are collected during the scan, then
 *       assigned in one columnar pass via {@link IndexRouting#indexShard(IndexRequest[], SourceBatch)}.
 *       This handles both cases where the batch already contains {@code _tsid} values (which are
 *       used as-is) and cases where it does not (where {@code indexShard} computes them via
 *       {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator}).
 *   <li><b>X-content per-item</b> — items carry inline JSON source. Each item is encoded and routed
 *       immediately by {@link BulkBatchEncoders}.
 * </ul>
 *
 * <p>Call {@link #route} for each item, then {@link #buildGrouping} to get the final
 * {@code Map<ShardId, List<BulkItemRequest>>}. Call {@link #shardBatches} separately for the
 * per-shard source data.
 */
final class BatchModeRouter implements Releasable {

    // TODO: restrict to a single concrete index for now; expand to multi-index in a follow-up.
    @Nullable
    private final String indexAbstractionName;
    @Nullable
    private final EscfBatch source;
    @Nullable
    private final int[] partitionIds;
    @Nullable
    private final BulkItemRequest[] items;
    @Nullable
    private Index concreteIndex;
    private int shardCount;
    private int lastRow = -1;
    private int routedCount;
    private boolean scattered;
    private boolean groupingBuilt;

    @Nullable
    private IndexRouting deferredRouting;

    // x-content mode state (null in provided-batch mode)
    @Nullable
    private final BulkBatchEncoders encoders;

    private BatchModeRouter(String indexAbstractionName, EscfBatch source) {
        this.indexAbstractionName = indexAbstractionName;
        this.source = source;
        this.partitionIds = new int[source.docCount()];
        this.items = new BulkItemRequest[source.docCount()];
        this.encoders = null;
    }

    private BatchModeRouter(BulkBatchEncoders encoders) {
        this.indexAbstractionName = null;
        this.source = null;
        this.partitionIds = null;
        this.items = null;
        this.encoders = encoders;
    }

    /** Returns the router for this bulk, or {@code null} when batch indexing does not apply. */
    @Nullable
    static BatchModeRouter create(BulkRequest bulkRequest, boolean batchIndexingSupported) {
        Map<String, SourceBatch> provided = bulkRequest.getPreBuiltBatches();
        boolean hasProvidedBatch = provided != null && provided.isEmpty() == false;

        if (hasProvidedBatch) {
            if (batchIndexingSupported == false) {
                throw new IllegalStateException(
                    "pre-built source batch submitted but batch indexing is not supported"
                        + " (setting disabled, feature flag off, or mixed-version cluster)"
                );
            }
            if (provided.size() > 1) {
                throw new IllegalArgumentException(
                    "pre-built source batch bulk carries "
                        + provided.size()
                        + " batches, but at most one is supported in step 1; multi-batch support will be added in a follow-up"
                );
            }
        } else if (batchIndexingSupported == false || bulkRequest.isSimulated() || bulkRequest.requests().isEmpty()) {
            return null;
        }

        // Single scan: both paths require all items to be IndexRequests; the provided-batch path
        // additionally requires every item to carry a source-row reference; the x-content path
        // requires each item to carry inline source with a known content type.
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            if (request instanceof IndexRequest indexRequest) {
                if (hasProvidedBatch) {
                    if (indexRequest.indexSource().hasSourceRow() == false) {
                        throw new IllegalArgumentException(
                            "item targeting index ["
                                + request.index()
                                + "] must carry a source-row reference when a pre-built batch is attached"
                        );
                    }
                } else if (BulkBatchEncoders.isItemBatchEligible(indexRequest) == false) {
                    return null;
                }
            } else {
                if (hasProvidedBatch) {
                    throw new IllegalArgumentException(
                        "["
                            + request.opType()
                            + "] operation on index ["
                            + request.index()
                            + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                            + " request carrying a source-row reference"
                    );
                }
                return null;
            }
        }

        if (hasProvidedBatch) {
            Map.Entry<String, SourceBatch> only = provided.entrySet().iterator().next();
            String name = only.getKey();
            SourceBatch batch = only.getValue();
            if (batch instanceof EscfBatch escfBatch) {
                return new BatchModeRouter(name, escfBatch);
            }
            throw new IllegalArgumentException(
                "pre-built batch for index [" + name + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
            );
        }

        return new BatchModeRouter(new BulkBatchEncoders());
    }

    /**
     * Records one item for routing. For the provided-batch mode the actual shard assignment is
     * deferred until {@link #buildGrouping}; for x-content the item is encoded and routed immediately.
     *
     * @param requestsByShard the grouping map to fill; both modes write into it
     */
    void route(
        BulkItemRequest bulkItem,
        DocWriteRequest<?> request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project,
        Map<ShardId, List<BulkItemRequest>> requestsByShard
    ) {
        if (encoders != null) {
            request.preRoutingProcess(routing);
            int shardId = encoders.tryEncodeAndRoute((IndexRequest) request, concreteIndex, routing);
            if (shardId == BulkBatchEncoders.NOT_BATCHABLE) {
                shardId = request.route(routing);
            }
            request.postRoutingProcess(routing);
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(bulkItem);
        } else {
            IndexRequest batchItem = (IndexRequest) request;
            prepareRouting(batchItem, abstraction, concreteIndex, routing, project);
            recordDeferredItem(bulkItem, batchItem.indexSource().rowIndex());
        }
    }

    private void prepareRouting(
        IndexRequest request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        if (indexAbstractionName.equals(abstraction.getName()) == false) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                    + " batches must be keyed by the name set on the requests whose rows they hold"
            );
        }
        if (this.concreteIndex == null) {
            assignConcrete(concreteIndex, routing, project);
        } else if (this.concreteIndex.equals(concreteIndex) == false) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + indexAbstractionName
                    + "] resolved to concrete index ["
                    + concreteIndex.getName()
                    + "] in addition to ["
                    + this.concreteIndex.getName()
                    + "]; batches spanning multiple concrete indices (e.g. TSDB data streams with"
                    + " multiple backing indices) are not yet supported and will be added in a follow-up"
            );
        }
    }

    private void assignConcrete(Index index, IndexRouting routing, ProjectMetadata project) {
        if (routing instanceof IndexRouting.ExtractFromSource) {
            if (routing instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
                throw new IllegalArgumentException(
                    "index ["
                        + index.getName()
                        + "] routes by extracting fields from _source, but this bulk supplies a pre-built source batch"
                        + " with no inline source; supply a pre-computed _tsid or use an index whose routing depends"
                        + " only on _id/_routing"
                );
            }
        }
        shardCount = project.getIndexSafe(index).getNumberOfShards();
        concreteIndex = index;
        this.deferredRouting = routing;
    }

    private void recordDeferredItem(BulkItemRequest bulkItem, int rowIndex) {
        int docCount = source.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + indexAbstractionName + "]"
            );
        }
        if (rowIndex <= lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + lastRow
                    + " of pre-built batch ["
                    + indexAbstractionName
                    + "]; rows must arrive in ascending order"
            );
        }
        lastRow = rowIndex;
        items[rowIndex] = bulkItem;
        routedCount++;
    }

    /**
     * Returns the final per-shard grouping. For provided-batch mode this resolves all deferred shard
     * assignments in one batch call via the columnar routing trio; for x-content the grouping was
     * built incrementally during {@link #route} calls. Must be called exactly once, after all
     * {@link #route} calls.
     *
     * <p>When the columnar routing trio throws, the failure is <em>batch-granular</em>: every
     * deferred item is reported via {@code onItemFailure} with the same exception, and an empty
     * grouping is returned. True per-item isolation would require
     * {@link IndexRouting#indexShard(IndexRequest[], SourceBatch)} /
     * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator} to identify the failing row —
     * a follow-up improvement.
     *
     * @param requestsByShard the grouping map to fill (the same map passed to each {@link #route} call)
     * @param onItemFailure   called for each deferred item when columnar routing fails as a whole
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        assert groupingBuilt == false : "buildGrouping called more than once";
        groupingBuilt = true;
        if (encoders != null) {
            return requestsByShard;
        }
        if (routedCount == 0) {
            return requestsByShard;
        }
        if (routedCount != source.docCount()) {
            throw new IllegalStateException(
                "pre-built batch ["
                    + indexAbstractionName
                    + "] had "
                    + source.docCount()
                    + " rows but only "
                    + routedCount
                    + " were routed; dropped rows in pre-built batches are not yet supported and will be added in a follow-up"
            );
        }
        IndexRequest[] requests = buildRequestArray();
        try {
            deferredRouting.preProcess(requests);
            int[] shards = deferredRouting.indexShard(requests, source);
            deferredRouting.postProcess(requests);

            for (int i = 0; i < requests.length; i++) {
                int shardId = shards[i];
                partitionIds[i] = shardId;
                requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(items[i]);
            }
        } catch (Exception e) {
            // The trio does not give us a row index, so we cannot isolate which row(s) caused the
            // problem. Fail every deferred item with the same exception.
            scattered = true; // prevent shardBatches() from attempting a stale scatter
            for (int i = 0; i < source.docCount(); i++) {
                if (items[i] != null) {
                    onItemFailure.accept(items[i], e);
                }
            }
        }
        return requestsByShard;
    }

    private IndexRequest[] buildRequestArray() {
        IndexRequest[] requests = new IndexRequest[routedCount];
        for (int i = 0; i < routedCount; i++) {
            requests[i] = (IndexRequest) items[i].request();
        }
        return requests;
    }

    /**
     * Returns the per-shard batches. For provided-batch mode, returns empty on any call after the
     * first — the failure-store redirect pass must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (encoders != null) {
            return encoders.finalizeBatches();
        }
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        if (routedCount == 0) {
            return Map.of();
        }
        if (shardCount == 1) {
            return Map.of(new ShardId(concreteIndex, 0), source);
        }
        return scatter();
    }

    private Map<ShardId, SourceBatch> scatter() {
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(source, partitionIds, shardCount);
        }
        Map<ShardId, SourceBatch> result = new HashMap<>();
        int[] nextRow = new int[shardCount];
        for (int row = 0; row < routedCount; row++) {
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row;
            result.putIfAbsent(new ShardId(concreteIndex, partition), part);
            IndexRequest req = (IndexRequest) items[row].request();
            req.indexSource().setSourceRow(part, nextRow[partition]++, req.indexSource().contentType());
        }
        return result;
    }

    /**
     * Verifies 1:1 alignment between shard items and their batch rows. The wire format rebuilds row
     * numbers from item ordinal, so misalignment would silently index the wrong source.
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
        if (encoders != null) {
            encoders.close();
        }
    }
}
