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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
public class Reconfigurator {

    private static final Logger logger = LogManager.getLogger(Reconfigurator.class);

    /**
     * The cluster usually requires a vote from at least half of the master nodes in order to commit a cluster state update, and to achieve
     * the best resilience it makes automatic adjustments to the voting configuration as master nodes join or leave the cluster. Adjustments
     * that fix or increase the size of the voting configuration are always a good idea, but the wisdom of reducing the voting configuration
     * size is less clear. For instance, automatically reducing the voting configuration down to a single node means the cluster requires
     * this node to operate, which is not resilient: if it broke we could restore every other master-eligible node in the cluster to health
     * and still the cluster would be unavailable. However not reducing the voting configuration size can also hamper resilience: in a
     * five-node cluster we could lose two nodes and by reducing the voting configuration to the remaining three nodes we could tolerate the
     * loss of a further node before failing.
     *
     * We offer two options: either we auto-shrink the voting configuration as long as it contains more than three nodes, or we don't and we
     * require the user to control the voting configuration manually using the retirement API. The former, default, option, guarantees that
     * as long as there have been at least three master-eligible nodes in the cluster and no more than one of them is currently unavailable,
     * then the cluster will still operate, which is what almost everyone wants. Manual control is for users who want different guarantees.
     */
    public static final Setting<Boolean> CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION =
        Setting.boolSetting("cluster.auto_shrink_voting_configuration", true, Property.NodeScope, Property.Dynamic);

    private volatile boolean autoShrinkVotingConfiguration;

    public Reconfigurator(Settings settings, ClusterSettings clusterSettings) {
        autoShrinkVotingConfiguration = CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION, this::setAutoShrinkVotingConfiguration);
    }

    public void setAutoShrinkVotingConfiguration(boolean autoShrinkVotingConfiguration) {
        this.autoShrinkVotingConfiguration = autoShrinkVotingConfiguration;
    }

    private static int roundDownToOdd(int size) {
        return size - (size % 2 == 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Reconfigurator{" +
            "autoShrinkVotingConfiguration=" + autoShrinkVotingConfiguration +
            '}';
    }

    /**
     * Compute an optimal configuration for the cluster.
     *
     * @param liveNodes      The live nodes in the cluster. The optimal configuration prefers live nodes over non-live nodes as far as
     *                       possible.
     * @param retiredNodeIds Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     *                       retired and not in the current configuration will never appear in the resulting configuration; this is useful
     *                       for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.
     * @param currentConfig  The current configuration. As far as possible, we prefer to keep the current config as-is.
     * @return An optimal configuration, or leave the current configuration unchanged if the optimal configuration has no live quorum.
     */
    public VotingConfiguration reconfigure(Set<DiscoveryNode> liveNodes, Set<String> retiredNodeIds, VotingConfiguration currentConfig) {
        logger.trace("{} reconfiguring {} based on liveNodes={}, retiredNodeIds={}", this, currentConfig, liveNodes, retiredNodeIds);

        /*
         *  There are three true/false properties of each node in play: live/non-live, retired/non-retired and in-config/not-in-config.
         *  Firstly we divide the nodes into disjoint sets based on these properties:
         *
         *  - nonRetiredInConfigNotLiveIds
         *  - nonRetiredInConfigLiveIds
         *  - nonRetiredLiveNotInConfigIds
         *
         *  The other 5 possibilities are not relevant:
         *  - retired, in-config, live             -- retired nodes should be removed from the config
         *  - retired, in-config, non-live         -- retired nodes should be removed from the config
         *  - retired, not-in-config, live         -- cannot add a retired node back to the config
         *  - retired, not-in-config, non-live     -- cannot add a retired node back to the config
         *  - non-retired, non-live, not-in-config -- no evidence this node exists at all
         */

        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet());
        final Set<String> liveInConfigIds = new TreeSet<>(currentConfig.getNodeIds());
        liveInConfigIds.retainAll(liveNodeIds);

        final Set<String> inConfigNotLiveIds = Sets.sortedDifference(currentConfig.getNodeIds(), liveInConfigIds);
        final Set<String> nonRetiredInConfigNotLiveIds = new TreeSet<>(inConfigNotLiveIds);
        nonRetiredInConfigNotLiveIds.removeAll(retiredNodeIds);

        final Set<String> nonRetiredInConfigLiveIds = new TreeSet<>(liveInConfigIds);
        nonRetiredInConfigLiveIds.removeAll(retiredNodeIds);

        final Set<String> nonRetiredLiveNotInConfigIds = Sets.sortedDifference(liveNodeIds, currentConfig.getNodeIds());
        nonRetiredLiveNotInConfigIds.removeAll(retiredNodeIds);

        /*
         * Now we work out how many nodes should be in the configuration:
         */
        final int targetSize;

        final int nonRetiredLiveNodeCount = nonRetiredInConfigLiveIds.size() + nonRetiredLiveNotInConfigIds.size();
        final int nonRetiredConfigSize = nonRetiredInConfigLiveIds.size() + nonRetiredInConfigNotLiveIds.size();
        if (autoShrinkVotingConfiguration) {
            if (nonRetiredLiveNodeCount >= 3) {
                targetSize = roundDownToOdd(nonRetiredLiveNodeCount);
            } else {
                // only have one or two available nodes; may not shrink below 3 nodes automatically, but if
                // the config (excluding retired nodes) is already smaller than 3 then it's ok.
                targetSize = nonRetiredConfigSize < 3 ? 1 : 3;
            }
        } else {
            targetSize = Math.max(roundDownToOdd(nonRetiredLiveNodeCount), nonRetiredConfigSize);
        }

        /*
         * The new configuration is formed by taking this many nodes in the following preference order:
         */
        final VotingConfiguration newConfig = new VotingConfiguration(
            // live nodes first, preferring the current config, and if we need more then use non-live nodes
            Stream.of(nonRetiredInConfigLiveIds, nonRetiredLiveNotInConfigIds, nonRetiredInConfigNotLiveIds)
                .flatMap(Collection::stream).limit(targetSize).collect(Collectors.toSet()));

        if (newConfig.hasQuorum(liveNodeIds)) {
            return newConfig;
        } else {
            // If there are not enough live nodes to form a quorum in the newly-proposed configuration, it's better to do nothing.
            return currentConfig;
        }
    }
}
