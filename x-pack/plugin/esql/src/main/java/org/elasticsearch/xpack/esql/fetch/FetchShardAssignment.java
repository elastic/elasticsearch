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
 * Hands the open shards of one fetch request to the drivers of its fetch plan, one shard per driver, in request order.
 * <p>
 * The sink of a driver collects the pages of the driver's shard, but it never sees a {@code _doc} column, which the fetch
 * plan drops before the sink. It finds its shard through the {@link DriverContext} the source of the same driver claimed
 * the shard with. The planner builds the source of a driver before its sink, so the shard is always known by then.
 */
final class FetchShardAssignment implements FetchDocsSourceOperator.ShardDocsProvider, FetchSourceProvider {
    private final List<FetchDocsSourceOperator.ShardDocs> shards;
    private final IndexedByShardId<? extends RefCounted> shardContexts;
    private final Map<DriverContext, Integer> shardOfDriver = new IdentityHashMap<>();
    private int nextShard;

    /**
     * @param shards        the open shards, by their position in the request
     * @param shardContexts the contexts of the shards by their position in the request, which the pages reference
     */
    FetchShardAssignment(List<FetchDocsSourceOperator.ShardDocs> shards, IndexedByShardId<? extends RefCounted> shardContexts) {
        this.shards = List.copyOf(shards);
        this.shardContexts = shardContexts;
    }

    @Override
    public synchronized FetchDocsSourceOperator.ShardDocs claim(DriverContext driverContext) {
        if (nextShard == shards.size()) {
            return null;
        }
        FetchDocsSourceOperator.ShardDocs shard = shards.get(nextShard++);
        shardOfDriver.put(driverContext, shard.shard());
        return shard;
    }

    /**
     * The position in the request of the shard the driver of {@code driverContext} loads.
     */
    synchronized int shardOf(DriverContext driverContext) {
        Integer shard = shardOfDriver.get(driverContext);
        if (shard == null) {
            throw new IllegalStateException("no shard was claimed for this fetch driver");
        }
        return shard;
    }

    @Override
    public IndexedByShardId<? extends RefCounted> refCounteds() {
        return shardContexts;
    }

    @Override
    public FetchSource fetchSource(FetchSourceExec exec, int maxPageSize) {
        return new FetchSource(new FetchDocsSourceOperator.Factory(this, maxPageSize), shards.size());
    }
}
