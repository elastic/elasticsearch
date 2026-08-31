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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-bulk router: decides each item's destination shard and builds the per-shard {@link SourceBatch}.
 * Provided-batch mode maps pre-built {@link EscfBatch} rows to shards and scatters; x-content mode
 * delegates encoding and routing to {@link BulkBatchEncoders} (TODO: temporary — goes away once all
 * producers build ESCF at the index-abstraction level).
 */
final class BatchModeRouter implements Releasable {

    // provided-batch mode state (null in x-content mode)
    // TODO: As a first pass we are restricting to a single concrete index. We will expand to multi index support.
    @Nullable
    private final String indexAbstractionName;
    @Nullable
    private final EscfBatch source;
    @Nullable
    private final int[] partitionIds;
    @Nullable
    private final IndexRequest[] items;
    @Nullable
    private Index concreteIndex;
    private int shardCount;
    private boolean requiresPrecomputedTsid;
    private int lastRow = -1;
    private int routedCount;
    private boolean scattered;

    // x-content mode state (null in provided-batch mode)
    @Nullable
    private final BulkBatchEncoders encoders;

    private BatchModeRouter(String indexAbstractionName, EscfBatch source) {
        this.indexAbstractionName = indexAbstractionName;
        this.source = source;
        this.partitionIds = new int[source.docCount()];
        this.items = new IndexRequest[source.docCount()];
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
     * Routes one item to its shard, performing all batch bookkeeping for this mode. Owns
     * {@code preRoutingProcess} / {@code postRoutingProcess} so {@link BulkOperation} needs a
     * single call.
     */
    int route(
        DocWriteRequest<?> request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        if (encoders != null) {
            request.preRoutingProcess(routing);
            int shardId = encoders.tryEncodeAndRoute((IndexRequest) request, concreteIndex, routing);
            if (shardId == BulkBatchEncoders.NOT_BATCHABLE) {
                shardId = request.route(routing);
            }
            request.postRoutingProcess(routing);
            return shardId;
        } else {
            IndexRequest batchItem = (IndexRequest) request;
            prepareRouting(batchItem, abstraction, concreteIndex, routing, project);
            request.preRoutingProcess(routing);
            int shardId = request.route(routing);
            request.postRoutingProcess(routing);
            markRoutedShard(batchItem, shardId);
            return shardId;
        }
    }

    /**
     * Validates that this item can be routed without {@code _source}, and binds the concrete index
     * on the first item. Must run before {@link IndexRequest#route(IndexRouting)}.
     */
    void prepareRouting(
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
        if (requiresPrecomputedTsid && request.tsid() == null) {
            throw new IllegalArgumentException(
                "index ["
                    + concreteIndex.getName()
                    + "] routes on _tsid, but this item of a pre-built source batch has no inline source to extract"
                    + " the dimensions from; supply a pre-computed _tsid"
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
            // hashSource short-circuits on a pre-computed tsid, which each item must therefore carry.
            requiresPrecomputedTsid = true;
        }
        shardCount = project.getIndexSafe(index).getNumberOfShards();
        concreteIndex = index;
    }

    void markRoutedShard(IndexRequest request, int shardId) {
        if (shardId < 0 || shardId >= shardCount) {
            throw new IllegalStateException(
                "shard [" + shardId + "] is outside the shard count [" + shardCount + "] of index [" + concreteIndex.getName() + "]"
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
     * Returns the per-shard batches. In provided-batch mode returns empty on any call after the
     * first — the failure-store redirect pass must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (encoders != null) {
            return encoders.finalizeBatches();
        }
        return providedBatchShardBatches();
    }

    private Map<ShardId, SourceBatch> providedBatchShardBatches() {
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        if (routedCount == 0) {
            return Map.of();
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
        for (int row = 0; row < partitionIds.length; row++) {
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row;
            result.putIfAbsent(new ShardId(concreteIndex, partition), part);
            items[row].indexSource().setSourceRow(part, nextRow[partition]++, items[row].indexSource().contentType());
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
