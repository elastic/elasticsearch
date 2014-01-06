package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 */
public class EnableAllocationDecider extends AllocationDecider implements NodeSettingsService.Listener {

    public static final String CLUSTER_ROUTING_ALLOCATION_ENABLE = "cluster.routing.allocation.enable";
    public static final String INDEX_ROUTING_ALLOCATION_ENABLE = "index.routing.allocation.enable";

    private volatile Allocation enable;

    @Inject
    public EnableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, Allocation.ALL.id()));
        nodeSettingsService.addListener(this);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return Decision.YES;
        }

        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        Allocation enable = Allocation.parse(indexSettings.get(INDEX_ROUTING_ALLOCATION_ENABLE, this.enable.id()));
        if (enable == null) { // Invalid enable setting value then default to all.
            enable = Allocation.ALL;
        }

        switch (enable) {
            case NONE:
                return Decision.NO;
            case NEW_PRIMARIES:
                if (shardRouting.primary() && !allocation.routingNodes().routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi()) {
                    return Decision.YES;
                } else {
                    return Decision.NO;
                }
            case PRIMARIES:
                return shardRouting.primary() ? Decision.YES : Decision.NO;
        }

        return Decision.YES;
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        Allocation enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, this.enable.id()));
        if (enable != this.enable) {
            logger.info("updating [cluster.routing.allocation.enable] from [{}] to [{}]", this.enable.id(), enable.id());
            EnableAllocationDecider.this.enable = enable;
        }
    }

    public enum Allocation {

        NONE("none"),
        NEW_PRIMARIES("new_primaries"),
        PRIMARIES("primaries"),
        ALL("all");

        private final String id;

        Allocation(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }

        public static Allocation parse(String strValue) {
            if ("none".equals(strValue)) {
                return NONE;
            } else if ("new_primaries".equals(strValue)) {
                return NEW_PRIMARIES;
            } else if ("primaries".equals(strValue)) {
                return PRIMARIES;
            } else if ("all".equals(strValue)) {
                return ALL;
            } else {
                return null;
            }
        }
    }

}
