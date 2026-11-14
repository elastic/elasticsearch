/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * Orders the shards of a data-node request by the {@code @timestamp} of their index, most recent first, or oldest first when
 * the query sorts on {@code @timestamp} ascending. The data node runs its shards in this order, so a query sorted on
 * {@code @timestamp} reads the shards most likely to make its top N first, and can skip the others.
 */
final class ShardTimestampOrder {

    private static final String FIELD = DataStream.TIMESTAMP_FIELD_NAME;

    private ShardTimestampOrder() {}

    /**
     * Orders the shards by their key, the largest first, or the smallest first when {@code ascending}.
     */
    static List<DataNodeRequest.Shard> order(List<DataNodeRequest.Shard> shards, ToLongFunction<ShardId> keys, boolean ascending) {
        record Keyed(DataNodeRequest.Shard shard, long key) {}
        List<Keyed> keyed = new ArrayList<>(shards.size());
        for (DataNodeRequest.Shard shard : shards) {
            keyed.add(new Keyed(shard, keys.applyAsLong(shard.shardId())));
        }
        Comparator<Keyed> byKey = Comparator.comparingLong(Keyed::key);
        keyed.sort(ascending ? byKey : byKey.reversed());
        return keyed.stream().map(Keyed::shard).toList();
    }

    /**
     * Whether the query sorts on {@code @timestamp} ascending.
     */
    static boolean ascending(PhysicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof FragmentExec fragment && fragment.fragment().anyMatch(l -> l instanceof TopN topN && sortsAscending(topN))
        );
    }

    /**
     * The number of sort keys of the plan's top N, if the batches of shards can share its bound: the plan has a single top N,
     * with a field as its first sort key. {@code 0} otherwise.
     */
    static int sharedSortKeys(PhysicalPlan plan) {
        List<TopN> topNs = new ArrayList<>();
        plan.forEachDown(FragmentExec.class, fragment -> fragment.fragment().forEachDown(TopN.class, topNs::add));
        if (topNs.size() != 1 || (topNs.getFirst().order().getFirst().child() instanceof Attribute) == false) {
            return 0;
        }
        return topNs.getFirst().order().size();
    }

    private static boolean sortsAscending(TopN topN) {
        Order first = topN.order().getFirst();
        return first.direction() == Order.OrderDirection.ASC
            && first.child() instanceof NamedExpression named
            && named.name().equals(FIELD);
    }

    /**
     * The key of a shard of this node, read from the metadata of its index. A time-series index accepts a declared range of
     * {@code @timestamp}, so its key is the end of that range, or its start when {@code ascending}. Any other index is
     * keyed by its creation date: the backing indices of a data stream are created in the order of their timestamps. An
     * index without {@code @timestamp} holds only nulls, which sort first when descending and last when ascending.
     */
    static long key(IndicesService indicesService, ShardId shardId, boolean ascending) {
        IndexService indexService;
        try {
            indexService = indicesService.indexServiceSafe(shardId.getIndex());
        } catch (RuntimeException e) {
            // A shard that isn't on this node fails when it runs
            return ascending ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        if ((indexService.mapperService().fieldType(FIELD) instanceof DateFieldMapper.DateFieldType) == false) {
            return Long.MAX_VALUE;
        }
        IndexMetadata index = indexService.getMetadata();
        if (IndexMode.isTsdb(index.getIndexMode())) {
            return ascending ? index.getTimeSeriesStart().toEpochMilli() : index.getTimeSeriesEnd().toEpochMilli();
        }
        return index.getCreationDate();
    }
}
