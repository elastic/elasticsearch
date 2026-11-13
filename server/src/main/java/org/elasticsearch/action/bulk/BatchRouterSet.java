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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Coordinator for the {@link BatchModeRouter}s of one bulk. A bulk may carry pre-built {@link EscfBatch}es for
 * several index abstractions (data streams, plain indices, or aliases); each item is dispatched to the router
 * holding the batch of its abstraction, found by {@link BatchModeRouter#batchKey}. A bulk without pre-built
 * batches is handled by a single x-content mode router, which this set passes every item to.
 *
 * <p>When the bulk has exactly one router, the common case, this class delegates to it directly and skips the
 * key lookup.
 */
final class BatchRouterSet implements Releasable {

    /** The only router of the bulk; non-null when the bulk has exactly one, or in x-content mode. */
    @Nullable
    private final BatchModeRouter sole;

    /** Whether {@link #sole} is an x-content mode router, which takes every item without a batch lookup. */
    private final boolean xContent;

    /** The routers by {@link BatchModeRouter#key}; non-null only when the bulk has two or more pre-built batches. */
    @Nullable
    private final Map<String, BatchModeRouter> routers;

    private BatchRouterSet(BatchModeRouter sole, boolean xContent) {
        this.sole = sole;
        this.xContent = xContent;
        this.routers = null;
    }

    private BatchRouterSet(Map<String, BatchModeRouter> routers) {
        this.sole = null;
        this.xContent = false;
        this.routers = routers;
    }

    /**
     * Returns the router set for this bulk, or {@code null} when batch indexing does not apply. When the bulk
     * carries pre-built batches, the set owns them, taken from the request; if this throws, they are released.
     *
     * @throws IllegalStateException    if pre-built batches are present but batch indexing is not supported on this node
     * @throws IllegalArgumentException if any item is not an {@link IndexRequest} carrying a source-row reference,
     *                                  or if any batch is not an {@link EscfBatch}
     */
    @Nullable
    static BatchRouterSet create(BulkRequest bulkRequest, boolean batchIndexingSupported, Recycler<BytesRef> recycler) {
        if (bulkRequest.hasPreBuiltBatches() == false) {
            BatchModeRouter xContentRouter = BatchModeRouter.createForXContent(bulkRequest, batchIndexingSupported, recycler);
            return xContentRouter == null ? null : new BatchRouterSet(xContentRouter, true);
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
     * Creates a router set from pre-built batches, each keyed by the {@link BatchModeRouter#batchKey} of the items
     * that hold its rows. On success the set owns the batches; if this throws, the caller still owns all of them.
     *
     * @throws IllegalArgumentException if any batch is not an {@link EscfBatch}
     */
    private static BatchRouterSet forBatches(Map<String, SourceBatch> batches, Recycler<BytesRef> recycler) {
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            if (entry.getValue() instanceof EscfBatch == false) {
                throw new IllegalArgumentException(
                    "pre-built batch for index ["
                        + entry.getKey()
                        + "] must be an EscfBatch but was ["
                        + entry.getValue().getClass().getName()
                        + "]"
                );
            }
        }
        if (batches.size() == 1) {
            Map.Entry<String, SourceBatch> only = batches.entrySet().iterator().next();
            return new BatchRouterSet(new BatchModeRouter(only.getKey(), (EscfBatch) only.getValue(), recycler), false);
        }
        Map<String, BatchModeRouter> map = new HashMap<>(batches.size() * 2);
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            map.put(entry.getKey(), new BatchModeRouter(entry.getKey(), (EscfBatch) entry.getValue(), recycler));
        }
        return new BatchRouterSet(map);
    }

    /**
     * Resolves {@code @timestamp} from each router's ESCF columns and caches it on its items'
     * {@link IndexRequest}s. Must be called before {@link #route}.
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
     * Records one item for routing, with the router that holds the batch of the item's index abstraction.
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
        BatchModeRouter router = xContent ? sole : findRouter(abstraction, project);
        if (router == null) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                    + " batches must be keyed by the write target resolved from the target index of the requests whose rows they hold"
            );
        }
        router.route(bulkItem, request, abstraction, concreteIndex, routing, project, requestsByShard);
    }

    /** Completes routing for every router and populates {@code requestsByShard}. Must be called once per bulk. */
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
     * Returns the per-shard batches of every router. The caller owns every returned batch. May be called at most
     * once per bulk: the failure-store redirect pass must not call it, since redirected items are new requests
     * carrying inline source.
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
                for (final var entry : router.shardBatches().entrySet()) {
                    final var previous = result.put(entry.getKey(), entry.getValue());
                    assert previous == null : "routers of one bulk produced batches for the same shard [" + entry.getKey() + "]";
                }
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(result.values());
            }
        }
        return result;
    }

    /** Releases every router's source batch. Batches returned from {@link #shardBatches()} stay valid. */
    @Override
    public void close() {
        if (sole != null) {
            sole.close();
        } else {
            Releasables.close(routers.values());
        }
    }

    /**
     * Returns the router for the abstraction's {@link BatchModeRouter#batchKey}, or {@code null} if no batch was
     * supplied for it. The sole router still has its key compared, to catch an item that targets a different
     * abstraction than the one batch the bulk carries.
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
