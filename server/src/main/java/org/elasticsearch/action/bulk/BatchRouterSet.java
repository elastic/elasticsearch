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

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Holds the {@link BatchModeRouter} of one bulk and creates it: from the pre-built {@link EscfBatch} the request
 * carries, or in x-content mode when it carries none. A pre-built batch is registered under the
 * {@link BatchModeRouter#batchKey} of the items that hold its rows, and an item that targets another abstraction is
 * rejected; an x-content mode router takes every item.
 */
final class BatchRouterSet implements Releasable {

    private final BatchModeRouter router;

    /** Whether {@link #router} is an x-content mode router, which takes every item without a key check. */
    private final boolean xContent;

    private BatchRouterSet(BatchModeRouter router, boolean xContent) {
        this.router = router;
        this.xContent = xContent;
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

    private static BatchRouterSet forBatches(Map<String, SourceBatch> batches, Recycler<BytesRef> recycler) {
        if (batches.size() > 1) {
            throw new IllegalArgumentException(
                "pre-built source batch bulk carries "
                    + batches.size()
                    + " batches, but at most one is supported in step 1; multi-batch support will be added in a follow-up"
            );
        }
        Map.Entry<String, SourceBatch> only = batches.entrySet().iterator().next();
        if (only.getValue() instanceof EscfBatch escfBatch) {
            return new BatchRouterSet(new BatchModeRouter(only.getKey(), escfBatch, recycler), false);
        }
        throw new IllegalArgumentException(
            "pre-built batch for index [" + only.getKey() + "] must be an EscfBatch but was [" + only.getValue().getClass().getName() + "]"
        );
    }

    /**
     * Resolves {@code @timestamp} from each router's ESCF columns and caches it on its items'
     * {@link IndexRequest}s. Must be called before {@link #route}.
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        router.preResolveTimestamps(project, requests);
    }

    /**
     * Records one item for routing.
     *
     * @throws IllegalArgumentException if the item targets an index abstraction other than the one the batch is for
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
        if (xContent == false) {
            String batchKey = BatchModeRouter.batchKey(abstraction, project);
            if (batchKey == null || batchKey.equals(router.key) == false) {
                throw new IllegalArgumentException(
                    "item targeting index ["
                        + request.index()
                        + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                        + " batches must be keyed by the write target resolved from the target index of the requests whose rows they hold"
                );
            }
        }
        router.route(bulkItem, request, abstraction, concreteIndex, routing, project, requestsByShard);
    }

    /** Completes routing and populates {@code requestsByShard}. Must be called once per bulk. */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        return router.buildGrouping(requestsByShard, onItemFailure);
    }

    /**
     * Returns the per-shard batches. The caller owns every returned batch. May be called at most once per bulk: the
     * failure-store redirect pass must not call it, since redirected items are new requests carrying inline source.
     *
     * @throws IllegalStateException if called more than once
     */
    Map<ShardId, SourceBatch> shardBatches() {
        return router.shardBatches();
    }

    /** Releases the router's source batch. Batches returned from {@link #shardBatches()} stay valid. */
    @Override
    public void close() {
        router.close();
    }
}
