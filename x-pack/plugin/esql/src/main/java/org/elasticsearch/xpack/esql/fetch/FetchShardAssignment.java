/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.planner.FetchSourceProvider;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hands the pieces of the open shards of one fetch request to the drivers of its fetch plan, one piece per driver, in
 * request order.
 * <p>
 * The sink of a driver collects the pages of the driver's piece, but it never sees a {@code _doc} column, which the fetch
 * plan drops before the sink. It finds its piece through the {@link DriverContext} the source of the same driver claimed
 * the piece with. The planner builds the source of a driver before its sink, so the piece is always known by then.
 */
final class FetchShardAssignment implements FetchDocsSourceOperator.ShardDocsProvider, FetchSourceProvider {
    private final List<FetchDocsSourceOperator.ShardDocs> pieces;
    private final IndexedByShardId<? extends RefCounted> shardContexts;
    private final Map<DriverContext, Integer> pieceOfDriver = new IdentityHashMap<>();
    private int nextPiece;

    /**
     * @param pieces        the pieces of the open shards, each with the position of its shard in the request
     * @param shardContexts the contexts of the shards by their position in the request, which the pages reference
     */
    FetchShardAssignment(List<FetchDocsSourceOperator.ShardDocs> pieces, IndexedByShardId<? extends RefCounted> shardContexts) {
        this.pieces = List.copyOf(pieces);
        this.shardContexts = shardContexts;
    }

    @Override
    public synchronized FetchDocsSourceOperator.ShardDocs claim(DriverContext driverContext) {
        if (nextPiece == pieces.size()) {
            return null;
        }
        int piece = nextPiece++;
        pieceOfDriver.put(driverContext, piece);
        return pieces.get(piece);
    }

    /**
     * The position in {@code pieces} of the piece the driver of {@code driverContext} loads.
     */
    synchronized int pieceOf(DriverContext driverContext) {
        Integer piece = pieceOfDriver.get(driverContext);
        if (piece == null) {
            throw new IllegalStateException("no piece was claimed for this fetch driver");
        }
        return piece;
    }

    @Override
    public IndexedByShardId<? extends RefCounted> refCounteds() {
        return shardContexts;
    }

    @Override
    public FetchSource fetchSource(FetchSourceExec exec, int maxPageSize) {
        return new FetchSource(new FetchDocsSourceOperator.Factory(this, maxPageSize), pieces.size());
    }
}
