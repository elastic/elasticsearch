/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Setting;

/**
 * Cheap shards, phase 3 ("both tiers to zero"): keeps the indexing (promotable) copy of a <em>parked</em> index unassigned, so that no
 * node holds an {@code IndexService}/{@code IndexShard} for it while the index stays open. Search copies are not affected.
 * <p>
 * The decider never unassigns anything by itself ({@code canRemain} is always YES): parking is "set {@link #PARKED_SETTING}, then fail
 * the started primary once" (e.g. reroute cancel with {@code allow_primary}); from then on the balancer leaves the primary unassigned
 * ({@code DECIDERS_NO}, index health RED). Unparking is setting {@link #PARKED_SETTING} back to false: the settings update reroutes and
 * the primary recovers from the object store with primary term + 1, as after a node failure. Default off: an index without the setting
 * is never touched.
 */
public class CheapShardParkedAllocationDecider extends AllocationDecider {

    public static final String NAME = "cheap_shard_parked";

    public static final Setting<Boolean> PARKED_SETTING = Setting.boolSetting(
        "index.cheap_shard.parked",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    private static final Decision YES_NOT_PARKED = Decision.single(
        Decision.Type.YES,
        NAME,
        "index is not parked or shard is not promotable"
    );
    private static final Decision NO_PARKED = Decision.single(
        Decision.Type.NO,
        NAME,
        "index is parked ([index.cheap_shard.parked] is true): its indexing shard stays unassigned until the setting is cleared"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return decide(shardRouting, allocation.metadata().indexMetadata(shardRouting.index()));
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // the default delegates to canAllocate only for NO -> force, make sure a parked primary is never force-allocated either
        return canAllocate(shardRouting, allocation);
    }

    static Decision decide(ShardRouting shardRouting, IndexMetadata indexMetadata) {
        if (shardRouting.isPromotableToPrimary() && indexMetadata != null && PARKED_SETTING.get(indexMetadata.getSettings())) {
            return NO_PARKED;
        }
        return YES_NOT_PARKED;
    }
}
