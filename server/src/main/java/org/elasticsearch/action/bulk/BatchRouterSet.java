/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Coordinator for one or more {@link BatchModeRouter} instances. A bulk request may carry
 * {@link EscfBatch}es for multiple index abstractions (data streams, plain indices, or aliases);
 * this class dispatches each item to the correct per-abstraction router.
 *
 * <p>When the bulk targets exactly one index abstraction — the common case — this class short-circuits
 * the key lookup and delegates directly to the sole {@link BatchModeRouter}, avoiding map overhead.
 */
final class BatchRouterSet implements Releasable {

    /** Fast path: non-null only when the bulk has exactly one index abstraction. */
    @Nullable
    private final BatchModeRouter sole;

    /** Multi-abstraction path: non-null only when the bulk has two or more index abstractions. */
    @Nullable
    private final Map<String, BatchModeRouter> routers;

    private BatchRouterSet(BatchModeRouter sole) {
        this.sole = sole;
        this.routers = null;
    }

    private BatchRouterSet(Map<String, BatchModeRouter> routers) {
        this.sole = null;
        this.routers = routers;
    }

    /**
     * Returns a router set when the bulk carries externally pre-built ESCF batches, or {@code null}
     * when no pre-built batches are present. On success the router set owns the batches, taken from
     * the request; on failure they are released here.
     *
     * @throws IllegalStateException    if pre-built batches are present but batch indexing is not
     *                                  supported on this node
     * @throws IllegalArgumentException if any item is not an {@link IndexRequest} carrying a
     *                                  source-row reference, or if any batch is not an
     *                                  {@link EscfBatch}
     */
    @Nullable
    static BatchRouterSet create(BulkRequest bulkRequest, boolean batchIndexingSupported, Recycler<BytesRef> recycler) {
        if (bulkRequest.hasPreBuiltBatches() == false) {
            return null;
        }
        if (batchIndexingSupported == false) {
            throw new IllegalStateException(
                "pre-built source batch submitted but batch indexing is not supported"
                    + " (setting disabled, feature flag off, or mixed-version cluster)"
            );
        }
        Map<String, SourceBatch> provided = bulkRequest.takePreBuiltBatches();
        boolean success = false;
        try {
            // Every item in a batch bulk must carry a source-row reference.
            for (DocWriteRequest<?> request : bulkRequest.requests()) {
                if (request instanceof IndexRequest indexRequest) {
                    if (indexRequest.indexSource().hasSourceRow() == false) {
                        throw new IllegalArgumentException(
                            "item targeting index ["
                                + request.index()
                                + "] must carry a source-row reference when a pre-built batch is attached"
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "["
                            + request.opType()
                            + "] operation on index ["
                            + request.index()
                            + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                            + " request carrying a source-row reference"
                    );
                }
            }
            BatchRouterSet set = forBatches(provided, recycler);
            success = true;
            return set;
        } finally {
            if (success == false) {
                Releasables.close(provided.values());
            }
        }
    }

    /**
     * Creates a router set from a map of pre-built {@link SourceBatch}es. Each key is the string
     * returned by {@link BatchModeRouter#batchKey} for the corresponding abstraction. On success the
     * router set owns the batches; if this throws, the caller still owns all of them.
     *
     * @throws IllegalArgumentException if any batch is not an {@link EscfBatch}
     */
    static BatchRouterSet forBatches(Map<String, SourceBatch> batches, Recycler<BytesRef> recycler) {
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            if (entry.getValue() instanceof EscfBatch == false) {
                throw new IllegalArgumentException(
                    "pre-built batch for ["
                        + entry.getKey()
                        + "] must be an EscfBatch but was ["
                        + entry.getValue().getClass().getName()
                        + "]"
                );
            }
        }
        if (batches.size() == 1) {
            Map.Entry<String, SourceBatch> entry = batches.entrySet().iterator().next();
            return new BatchRouterSet(toRouter(entry.getKey(), entry.getValue(), recycler));
        }
        Map<String, BatchModeRouter> map = new HashMap<>(batches.size() * 2);
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            map.put(entry.getKey(), toRouter(entry.getKey(), entry.getValue(), recycler));
        }
        return new BatchRouterSet(map);
    }

    private static BatchModeRouter toRouter(String key, SourceBatch batch, Recycler<BytesRef> recycler) {
        if (batch instanceof EscfBatch escfBatch) {
            return new BatchModeRouter(key, escfBatch, recycler);
        }
        throw new IllegalArgumentException(
            "pre-built batch for [" + key + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
        );
    }

    /**
     * Resolves {@code @timestamp} from each router's ESCF columns and caches it on each item's
     * {@link IndexRequest}. Must be called before {@link #route} so that backing-index selection can
     * use the memoized value.
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        if (sole != null) {
            sole.preResolveTimestamps(project, requests);
        } else {
            for (BatchModeRouter router : routers.values()) {
                router.preResolveTimestamps(project, requests);
            }
        }
    }

    /**
     * Records one item for deferred routing. Dispatches to the correct per-abstraction
     * {@link BatchModeRouter} via the batch key computed from the item's index abstraction.
     *
     * <p>An {@link IndexRequest} without a source-row reference belongs to an index that did not opt in
     * to batch indexing ({@code index.batch_indexing}); {@link BulkBatchEncoders#encode} leaves such items
     * on their inline source. They are routed immediately by the row path and added to
     * {@code requestsByShard}. Pre-built batches never contain such items, as {@link #create} requires
     * every item to carry a source-row reference.
     *
     * <p>Every other item is deferred until {@link #buildGrouping}, so {@code requestsByShard} is only
     * written by this method for the inline items described above.
     *
     * @throws IllegalArgumentException if no batch was supplied for the item's index abstraction
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
        if (request instanceof IndexRequest indexRequest && indexRequest.indexSource().hasSourceRow() == false) {
            request.preRoutingProcess(routing);
            int shardId = request.route(routing);
            request.postRoutingProcess(routing);
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(bulkItem);
            return;
        }
        BatchModeRouter router = findRouter(abstraction, project);
        if (router == null) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied for it;"
                    + " batches must be keyed by the write-target resolved from the request's target index"
            );
        }
        router.route(bulkItem, (IndexRequest) request, concreteIndex, routing, project);
    }

    /**
     * Completes routing for all per-abstraction routers and populates {@code requestsByShard}. Must
     * be called exactly once per bulk request.
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        if (sole != null) {
            sole.buildGrouping(requestsByShard, onItemFailure);
        } else {
            for (BatchModeRouter router : routers.values()) {
                router.buildGrouping(requestsByShard, onItemFailure);
            }
        }
        return requestsByShard;
    }

    /**
     * Returns the per-shard batches for all abstractions. The caller owns every returned batch. May be
     * called at most once per bulk: the failure-store redirect pass must not call it, since redirected
     * items are new requests carrying inline source.
     *
     * @throws IllegalStateException if called more than once
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (sole != null) {
            return sole.shardBatches();
        }
        Map<ShardId, SourceBatch> result = new HashMap<>();
        boolean success = false;
        try {
            for (BatchModeRouter router : routers.values()) {
                result.putAll(router.shardBatches());
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(result.values());
            }
        }
        return result;
    }

    /**
     * Verifies 1:1 alignment between shard items and their batch rows. The wire format rebuilds row
     * numbers from item ordinal, so misalignment would silently index the wrong source.
     */
    static void validateBatchAlignment(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            List<BulkItemRequest> shardItems = entry.getValue();
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            if (shardBatch == null) {
                for (BulkItemRequest item : shardItems) {
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
            } else if (BulkShardBatch.rowsAlignWithItems(shardBatch, shardItems) == false) {
                throw new IllegalStateException(
                    "batch for shard ["
                        + entry.getKey()
                        + "] does not align with its items (batch rows: "
                        + shardBatch.docCount()
                        + ", items: "
                        + shardItems.size()
                        + "); this indicates a bug in the scatter logic"
                );
            }
        }
    }

    /**
     * Releases every router's source batch. Safe to call more than once. Batches returned from {@link #shardBatches()} stay valid; their owner
     * releases them.
     */
    @Override
    public void close() {
        if (sole != null) {
            sole.close();
        } else {
            Releasables.close(routers.values());
        }
    }

    /**
     * Returns the router for the given abstraction, or {@code null} if no batch was supplied for it.
     *
     * <p>The sole fast path still performs a key comparison to guard against an item targeting a
     * different abstraction than the one batch the bulk carries.
     */
    @Nullable
    private BatchModeRouter findRouter(IndexAbstraction abstraction, ProjectMetadata project) {
        String k = BatchModeRouter.batchKey(abstraction, project);
        if (sole != null) {
            return k != null && k.equals(sole.key) ? sole : null;
        }
        return k == null ? null : routers.get(k);
    }
}
