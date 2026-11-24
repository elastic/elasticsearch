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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Coordinator for the {@link BatchModeRouter}s of one bulk. A bulk may carry {@link EscfBatch}es for several index
 * abstractions (data streams, plain indices, or aliases), supplied by a producer or encoded from the items by
 * {@link BulkBatchEncoders#encode}; each item is dispatched to the router holding the batch of its abstraction, found
 * by the {@link BatchModeRouter#batchKey} of the abstraction.
 *
 * <p>When the bulk has exactly one router, the common case, this class delegates to it directly and skips the
 * key lookup.
 */
final class BatchRouterSet implements Releasable {

    /** The only router of the bulk; non-null when the bulk has exactly one. */
    @Nullable
    private final BatchModeRouter sole;

    /** The routers by {@link BatchModeRouter#key}; non-null only when the bulk has two or more pre-built batches. */
    @Nullable
    private final Map<String, BatchModeRouter> routers;

    /**
     * The router of each batch, found by the batch itself, which is what an item's row points to, and not by a name. It
     * compares batches by identity: the question is which batch an item's row is in.
     */
    private final Map<SourceBatch, BatchModeRouter> routersByBatch;

    private BatchRouterSet(BatchModeRouter sole, Map<SourceBatch, BatchModeRouter> routersByBatch) {
        this.sole = sole;
        this.routers = null;
        this.routersByBatch = routersByBatch;
    }

    private BatchRouterSet(Map<String, BatchModeRouter> routers, Map<SourceBatch, BatchModeRouter> routersByBatch) {
        this.sole = null;
        this.routers = routers;
        this.routersByBatch = routersByBatch;
    }

    /**
     * Returns a router set when the bulk carries externally pre-built ESCF batches, or {@code null} when no pre-built
     * batches are present. On success the router set owns the batches, taken from the request; on failure they are
     * released here.
     *
     * @throws IllegalStateException    if pre-built batches are present but batch indexing is not supported on this node
     * @throws IllegalArgumentException if any item is not an {@link IndexRequest} carrying a source-row reference, if
     *                                  any batch is not an {@link EscfBatch}, if one batch is supplied under several
     *                                  keys, if an item holds a row of a batch that was not supplied, or if two items
     *                                  hold the same row of a batch
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
            // If this throws the set is not closed: the batches are released below, once each, which is all it would do.
            validateItemRows(bulkRequest.requests(), set);
            success = true;
            return set;
        } finally {
            if (success == false) {
                // The same batch may have been supplied under several keys, and must be released once.
                Set<SourceBatch> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
                distinct.addAll(provided.values());
                Releasables.close(distinct);
            }
        }
    }

    /**
     * Creates a router set from batches, each keyed by the {@link BatchModeRouter#batchKey} of the items that hold its
     * rows. On success the set owns the batches; if this throws, the caller still owns all of them.
     *
     * @throws IllegalArgumentException if any batch is not an {@link EscfBatch}, or if one is supplied under several keys
     */
    static BatchRouterSet forBatches(Map<String, SourceBatch> batches, Recycler<BytesRef> recycler) {
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
        Map<String, BatchModeRouter> routers = new HashMap<>(batches.size() * 2);
        Map<SourceBatch, BatchModeRouter> routersByBatch = new IdentityHashMap<>();
        for (Map.Entry<String, SourceBatch> entry : batches.entrySet()) {
            BatchModeRouter router = new BatchModeRouter(entry.getKey(), (EscfBatch) entry.getValue(), recycler);
            BatchModeRouter other = routersByBatch.put(entry.getValue(), router);
            if (other != null) {
                throw new IllegalArgumentException(
                    "the same pre-built batch was supplied for both ["
                        + other.key
                        + "] and ["
                        + entry.getKey()
                        + "]; each index abstraction needs a batch of its own"
                );
            }
            routers.put(entry.getKey(), router);
        }
        if (routers.size() == 1) {
            return new BatchRouterSet(routers.values().iterator().next(), routersByBatch);
        }
        return new BatchRouterSet(routers, routersByBatch);
    }

    /**
     * Checks the rows that the items hold: each is in one of the batches supplied, and no row is held by two items. A row
     * is the document of one item, so either would be a mistake of the producer, which would otherwise show up later as
     * one item losing its timestamp to another, or being rejected when it is routed.
     *
     * <p>A row that is out of range is left to be rejected when it is routed.
     *
     * @param requests the items of the bulk, each an {@link IndexRequest} with a source row
     * @throws IllegalArgumentException if an item holds a row of a batch that was not supplied, or if two items hold the
     *                                  same row of a batch
     */
    private static void validateItemRows(List<DocWriteRequest<?>> requests, BatchRouterSet set) {
        Map<BatchModeRouter, BitSet> rowsTaken = new HashMap<>();
        for (DocWriteRequest<?> request : requests) {
            final var indexSource = ((IndexRequest) request).indexSource();
            final var router = set.routersByBatch.get(indexSource.sourceBatch());
            if (router == null) {
                throw new IllegalArgumentException(
                    "item targeting index [" + request.index() + "] holds a row of a batch that was not supplied for the bulk"
                );
            }
            final int row = indexSource.rowIndex();
            if (row < router.source().docCount()) {
                BitSet taken = rowsTaken.computeIfAbsent(router, r -> new BitSet(r.source().docCount()));
                if (taken.get(row)) {
                    throw new IllegalArgumentException("two items hold row " + row + " of the pre-built batch for [" + router.key + "]");
                }
                taken.set(row);
            }
        }
    }

    /**
     * Resolves {@code @timestamp} from the ESCF columns of each router that needs it and caches it on its items'
     * {@link IndexRequest}s. Must be called before {@link #route}.
     *
     * <p>With several routers the rows of their batches overlap, so the requests are first sorted out by the batch
     * their row is in, in one pass over the bulk and only when some router needs timestamps. This does not go through
     * the names the items target, which {@link #route} resolves, as dates in them for one.
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        if (sole != null) {
            sole.preResolveTimestamps(project, requests);
            return;
        }
        Map<BatchModeRouter, List<DocWriteRequest<?>>> requestsByRouter = null;
        for (BatchModeRouter router : routers.values()) {
            if (router.resolvesTimestamps(project)) {
                if (requestsByRouter == null) {
                    requestsByRouter = new HashMap<>();
                }
                requestsByRouter.put(router, new ArrayList<>());
            }
        }
        if (requestsByRouter == null) {
            return;
        }
        for (DocWriteRequest<?> request : requests) {
            if (request instanceof IndexRequest indexRequest && indexRequest.indexSource().hasSourceRow()) {
                BatchModeRouter router = routersByBatch.get(indexRequest.indexSource().sourceBatch());
                List<DocWriteRequest<?>> routerRequests = router == null ? null : requestsByRouter.get(router);
                if (routerRequests != null) {
                    routerRequests.add(request);
                }
            }
        }
        for (Map.Entry<BatchModeRouter, List<DocWriteRequest<?>>> entry : requestsByRouter.entrySet()) {
            entry.getKey().preResolveTimestamps(project, entry.getValue());
        }
    }

    /**
     * Records one item for routing, with the router that holds the batch of the item's index abstraction.
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
     * @throws IllegalArgumentException if no batch was supplied for the item's index abstraction, or if the row of the
     *                                  item is in another batch than that one
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
            String batchKey = BatchModeRouter.batchKey(abstraction, project);
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied for ["
                    + (batchKey == null ? abstraction.getName() : batchKey)
                    + "]; a batch must be keyed by the write target of the index abstraction its items target: the data stream,"
                    + " or the write index"
            );
        }
        if (((IndexRequest) request).indexSource().sourceBatch() != router.source()) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] holds a row of a batch other than the pre-built batch supplied for ["
                    + router.key
                    + "]; the rows of an item must be in the batch registered under the key of the item"
            );
        }
        router.route(bulkItem, (IndexRequest) request, concreteIndex, routing, project);
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
