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

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Computes the optimal configuration of voting nodes in the cluster.
 */
public class Reconfigurator extends AbstractComponent {

    /**
     * The cluster usually requires a vote from at least half of the master nodes in order to commit a cluster state update, and to achieve
     * this it makes automatic adjustments to the quorum size as master nodes join or leave the cluster. However, if master nodes leave the
     * cluster slowly enough then these automatic adjustments can end up with a single master node; if this last node were to fail then the
     * cluster would be rendered permanently unavailable. Instead it may be preferable to stop processing cluster state updates and become
     * unavailable when the second-last (more generally, n'th-last) node leaves the cluster, so that the cluster is never in a situation
     * where a single node's failure can cause permanent unavailability. This setting determines the size of the smallest set of master
     * nodes required to process a cluster state update.
     */
    public static final Setting<Integer> MINIMUM_VOTING_MASTER_NODES_SETTING =
        Setting.intSetting("cluster.minimum_voting_master_nodes", 1, 1, Property.NodeScope, Property.Dynamic);

    private int minVotingMasterNodes;

    public Reconfigurator(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        minVotingMasterNodes = MINIMUM_VOTING_MASTER_NODES_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MINIMUM_VOTING_MASTER_NODES_SETTING, this::setMinVotingMasterNodes);
    }

    public void setMinVotingMasterNodes(int minVotingMasterNodes) {
        this.minVotingMasterNodes = minVotingMasterNodes;
    }

    private static int roundDownToOdd(int size) {
        return size - (size % 2 == 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Reconfigurator{" +
            "minVotingMasterNodes=" + minVotingMasterNodes +
            '}';
    }

    /**
     * Compute an optimal configuration for the cluster.
     * @param liveNodes The live nodes in the cluster. The optimal configuration prefers live nodes over non-live nodes as far as possible.
     * @param retiredNodeIds Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     *                       retired and not in the current configuration will never appear in the resulting configuration; this is useful
     *                       for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.
     * @param currentConfig The current configuration. As far as possible, we prefer to keep the current config as-is.
     * @return An optimal configuration, or leave the current configuration unchanged if the optimal configuration has no live quorum.
     */
    public ClusterState.VotingConfiguration reconfigure(Set<DiscoveryNode> liveNodes, Set<String> retiredNodeIds,
                                                        ClusterState.VotingConfiguration currentConfig) {
        logger.trace("{} reconfiguring {} based on liveNodes={}, retiredNodeIds={}", this, currentConfig, liveNodes, retiredNodeIds);

        /*
         *  There are three true/false properties of each node in play: live/non-live, retired/non-retired and in-config/not-in-config.
         *  Firstly we divide the nodes into disjoint sets based on these properties:
         *
         *  -    retiredInConfigNotLiveIds
         *  - nonRetiredInConfigNotLiveIds
         *  -    retiredInConfigLiveIds
         *  - nonRetiredInConfigLiveIds
         *  - nonRetiredLiveNotInConfigIds
         *
         *  The other 3 possibilities are not relevant:
         *  - retired, not-in-config, live         -- cannot add a retired node back to the config
         *  - retired, not-in-config, non-live     -- cannot add a retired node back to the config
         *  - non-retired, non-live, not-in-config -- no evidence this node exists at all
         */

        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet());
        final Set<String> liveInConfigIds = new TreeSet<>(currentConfig.getNodeIds());
        liveInConfigIds.retainAll(liveNodeIds);

        final Set<String> inConfigNotLiveIds = Sets.sortedDifference(currentConfig.getNodeIds(), liveInConfigIds);
        final Set<String> retiredInConfigNotLiveIds = new TreeSet<>(inConfigNotLiveIds);
        retiredInConfigNotLiveIds.retainAll(retiredNodeIds);
        final Set<String> nonRetiredInConfigNotLiveIds = new TreeSet<>(inConfigNotLiveIds);
        nonRetiredInConfigNotLiveIds.removeAll(retiredInConfigNotLiveIds);

        final Set<String> retiredInConfigLiveIds = new TreeSet<>(liveInConfigIds);
        retiredInConfigLiveIds.retainAll(retiredNodeIds);
        final Set<String> nonRetiredInConfigLiveIds = new TreeSet<>(liveInConfigIds);
        nonRetiredInConfigLiveIds.removeAll(retiredInConfigLiveIds);

        final Set<String> nonRetiredLiveNotInConfigIds = Sets.sortedDifference(liveNodeIds, currentConfig.getNodeIds());
        nonRetiredLiveNotInConfigIds.removeAll(retiredNodeIds);

        /*
         * Now we work out how many nodes should be in the configuration:
         */

        // ideally we want the configuration to be all the non-retired live nodes ...
        final int nonRetiredLiveNodeCount = nonRetiredInConfigLiveIds.size() + nonRetiredLiveNotInConfigIds.size();

        // ... except one, if even, because odd configurations are slightly more resilient ...
        final int votingNodeCount = roundDownToOdd(nonRetiredLiveNodeCount);

        // ... unless there's too few non-retired live nodes to meet the minimum configured resilience level:
        final int targetSize = Math.max(votingNodeCount, 2 * minVotingMasterNodes - 1);

        /*
         * The new configuration is formed by taking this many nodes in the following preference order:
         */
        final ClusterState.VotingConfiguration newConfig = new ClusterState.VotingConfiguration(
            Stream.of(nonRetiredInConfigLiveIds, nonRetiredLiveNotInConfigIds, // live nodes first, preferring the current config
                retiredInConfigLiveIds, // if we need more, first use retired nodes that are still alive and haven't been removed yet
                nonRetiredInConfigNotLiveIds, retiredInConfigNotLiveIds) // if we need more, use non-live nodes
                .flatMap(Collection::stream).limit(targetSize).collect(Collectors.toSet()));

        if (newConfig.hasQuorum(liveNodeIds)) {
            return newConfig;
        } else {
            // If there are not enough live nodes to form a quorum in the newly-proposed configuration, it's better to do nothing.
            return currentConfig;
        }
    }
}
