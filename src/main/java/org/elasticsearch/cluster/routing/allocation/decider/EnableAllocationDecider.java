/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

/**
 * This allocation decider allows shard allocations via the cluster wide settings {@link #CLUSTER_ROUTING_ALLOCATION_ENABLE}
 * and the per index setting {@link #INDEX_ROUTING_ALLOCATION_ENABLE}. The per index settings overrides the cluster wide
 * setting. Depending on the
 *
 * Both settings can have the following values:
 * <ul>
 *     <li> <code>NONE</code>, no shard allocation is allowed.
 *     <li> <code>NEW_PRIMARIES</code> only primary shards of new indices are allowed to be allocated
 *     <li> <code>PRIMARIES</code> only primary shards (of any index) are allowed to be allocated
 *     <li> <code>ALL</code> all shards are allowed to be allocated
 * </ul>
 */
public class EnableAllocationDecider extends AllocationDecider{

    public static final String NAME = "enable";

    public static final String CLUSTER_ROUTING_ALLOCATION_ENABLE = "cluster.routing.allocation.enable";
    public static final String INDEX_ROUTING_ALLOCATION_ENABLE = "index.routing.allocation.enable";

    public static final Allocation DEFAULT_ENABLE = Allocation.ALL;

    private volatile Allocation enable = DEFAULT_ENABLE;

    @Inject
    public EnableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new ApplySettings(settings));
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "allocation disabling is ignored");
        }

        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        String enableIndexValue = indexSettings.get(INDEX_ROUTING_ALLOCATION_ENABLE);
        final Allocation enable;
        if (enableIndexValue != null) {
            enable = Allocation.parse(enableIndexValue);
        } else {
            enable = this.enable;
        }
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all allocations are allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no allocations are allowed");
            case NEW_PRIMARIES:
                if (shardRouting.primary() && !allocation.routingNodes().routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi()) {
                    return allocation.decision(Decision.YES, NAME, "new primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "non-new primary allocations are disallowed");
                }
            case PRIMARIES:
                if (shardRouting.primary()) {
                    return allocation.decision(Decision.YES, NAME, "primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "replica allocations are disallowed");
                }
            default:
                throw new ElasticsearchIllegalStateException("Unknown allocation option");
        }
    }

    public enum Allocation {

        NONE,
        NEW_PRIMARIES,
        PRIMARIES,
        ALL;

        public static Allocation parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                try {
                    return Allocation.valueOf(strValue);
                } catch (IllegalArgumentException e) {
                    throw new ElasticsearchIllegalArgumentException("Illegal allocation.enable value [" + strValue + "]");
                }
            }
        }
    }

    class ApplySettings extends NodeSettingsService.Listener {
        public ApplySettings(Settings settings) {
            super(settings);
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            Allocation enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, DEFAULT_ENABLE.name()));
            if (enable != EnableAllocationDecider.this.enable) {
                logger.info("updating [cluster.routing.allocation.enable] from [{}] to [{}]", EnableAllocationDecider.this.enable, enable);
                EnableAllocationDecider.this.enable = enable;
            }
        }
    }

}
