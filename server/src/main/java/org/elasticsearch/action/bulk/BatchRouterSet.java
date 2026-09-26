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
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;

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
final class BatchRouterSet {

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
     * when no pre-built batches are present.
     *
     * @throws IllegalStateException    if pre-built batches are present but batch indexing is not
     *                                  supported on this node
     * @throws IllegalArgumentException if any item is not an {@link IndexRequest} carrying a
     *                                  source-row reference, or if any batch is not an
     *                                  {@link EscfBatch}
     */
    @Nullable
    static BatchRouterSet create(BulkRequest bulkRequest, boolean batchIndexingSupported) {
        Map<String, SourceBatch> provided = bulkRequest.getPreBuiltBatches();
        if (provided == null || provided.isEmpty()) {
            return null;
        }
        if (batchIndexingSupported == false) {
            throw new IllegalStateException(
                "pre-built source batch submitted but batch indexing is not supported"
                    + " (setting disabled, feature flag off, or mixed-version cluster)"
            );
        }

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

        return forBatches(provided);
    }

    /**
     * Creates a router set from a map of pre-built {@link SourceBatch}es. Each key is the string
     * returned by {@link BatchModeRouter#batchKey} for the corresponding abstraction.
     *
     * @throws IllegalArgumentException if any batch is not an {@link EscfBatch}
     */
    static BatchRouterSet forBatches(Map<String, SourceBatch> batches) {
        if (batches.size() == 1) {
            Map.Entry<String, SourceBatch> entry = batches.entrySet().iterator().next();
            return new BatchRouterSet(toRouter(entry.getKey(), entry.getValue()));
        }
        Map<String, BatchModeRouter> map = new HashMap<>(batches.size() * 2);
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            map.put(entry.getKey(), toRouter(entry.getKey(), entry.getValue()));
        }
        return new BatchRouterSet(map);
    }

    private static BatchModeRouter toRouter(String key, SourceBatch batch) {
        if (batch instanceof EscfBatch escfBatch) {
            return new BatchModeRouter(key, escfBatch);
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
     * <p>The {@code requestsByShard} parameter is accepted for API compatibility with the production
     * call site but is unused — items are deferred until {@link #buildGrouping}.
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
     * Returns the per-shard batches for all abstractions. Returns empty on any call after the first —
     * the failure-store redirect pass must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (sole != null) {
            return sole.shardBatches();
        }
        Map<ShardId, SourceBatch> result = new HashMap<>();
        for (BatchModeRouter router : routers.values()) {
            result.putAll(router.shardBatches());
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
     * No-op: source batches are owned by the caller (external pre-built) or, for the x-content
     * encode path, by the {@link org.elasticsearch.escf.EscfBatch} instances that survive in the
     * scattered sub-batches until the shard responses arrive.
     *
     * <p>TODO: introduce recycler-backed encoding and ref-counted batch lifetime so that internally
     * encoded batches are properly released after {@link BulkOperation#closeBatchEncoders}.
     */
    void close() {}

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
