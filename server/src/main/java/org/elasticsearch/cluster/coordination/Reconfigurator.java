/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
    public static final Setting<Boolean> CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION = Setting.boolSetting(
        "cluster.auto_shrink_voting_configuration",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    private volatile boolean autoShrinkVotingConfiguration;

    @SuppressWarnings("this-escape")
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
        return "Reconfigurator{" + "autoShrinkVotingConfiguration=" + autoShrinkVotingConfiguration + '}';
    }

    /**
     * Compute an optimal configuration for the cluster.
     *
     * @param liveNodes      The live nodes in the cluster. The optimal configuration prefers live nodes over non-live nodes as far as
     *                       possible.
     * @param retiredNodeIds Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     *                       retired and not in the current configuration will never appear in the resulting configuration; this is useful
     *                       for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.
     * @param currentMaster  The current master. Unless retired, we prefer to keep the current master in the config.
     * @param currentConfig  The current configuration. As far as possible, we prefer to keep the current config as-is.
     * @return An optimal configuration, or leave the current configuration unchanged if the optimal configuration has no live quorum.
     */
    public VotingConfiguration reconfigure(
        Set<DiscoveryNode> liveNodes,
        Set<String> retiredNodeIds,
        DiscoveryNode currentMaster,
        VotingConfiguration currentConfig
    ) {
        assert liveNodes.contains(currentMaster) : "liveNodes = " + liveNodes + " master = " + currentMaster;
        logger.trace(
            () -> Strings.format(
                "%s reconfiguring %s based on liveNodes=%s, retiredNodeIds=%s, currentMaster=%s",
                this,
                currentConfig,
                // Sorting the node IDs for deterministic logging until https://github.com/elastic/elasticsearch/issues/94946 is fixed
                liveNodes.stream().map(DiscoveryNode::toString).sorted().collect(Collectors.joining(", ", "[", "]")),
                retiredNodeIds.stream().sorted().collect(Collectors.joining(", ")),
                currentMaster
            )
        );

        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode)
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
        final Set<String> currentConfigNodeIds = currentConfig.getNodeIds();

        final Set<VotingConfigNode> orderedCandidateNodes = new TreeSet<>();
        liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode)
            .filter(n -> retiredNodeIds.contains(n.getId()) == false)
            .forEach(
                n -> orderedCandidateNodes.add(
                    new VotingConfigNode(n.getId(), true, n.getId().equals(currentMaster.getId()), currentConfigNodeIds.contains(n.getId()))
                )
            );
        currentConfigNodeIds.stream()
            .filter(nid -> liveNodeIds.contains(nid) == false)
            .filter(nid -> retiredNodeIds.contains(nid) == false)
            .forEach(nid -> orderedCandidateNodes.add(new VotingConfigNode(nid, false, false, true)));

        /*
         * Now we work out how many nodes should be in the configuration:
         */
        final int nonRetiredConfigSize = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.inCurrentConfig).count());
        final int minimumConfigEnforcedSize = autoShrinkVotingConfiguration ? (nonRetiredConfigSize < 3 ? 1 : 3) : nonRetiredConfigSize;
        final int nonRetiredLiveNodeCount = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.live).count());
        final int targetSize = Math.max(roundDownToOdd(nonRetiredLiveNodeCount), minimumConfigEnforcedSize);

        final VotingConfiguration newConfig = new VotingConfiguration(
            orderedCandidateNodes.stream().limit(targetSize).map(n -> n.id).collect(Collectors.toSet())
        );

        // new configuration should have a quorum
        if (newConfig.hasQuorum(liveNodeIds)) {
            return newConfig;
        } else {
            // If there are not enough live nodes to form a quorum in the newly-proposed configuration, it's better to do nothing.
            return currentConfig;
        }
    }

    public ClusterState maybeReconfigureAfterNewMasterIsElected(ClusterState clusterState) {
        return clusterState;
    }

    public void ensureVotingConfigCanBeModified() {
        // Temporary workaround until #98055 is tackled
        // no-op
    }

    record VotingConfigNode(String id, boolean live, boolean currentMaster, boolean inCurrentConfig)
        implements
            Comparable<VotingConfigNode> {

        @Override
        public int compareTo(VotingConfigNode other) {
            // prefer current master
            final int currentMasterComp = Boolean.compare(other.currentMaster, currentMaster);
            if (currentMasterComp != 0) {
                return currentMasterComp;
            }
            // prefer nodes that are live
            final int liveComp = Boolean.compare(other.live, live);
            if (liveComp != 0) {
                return liveComp;
            }
            // prefer nodes that are in current config for stability
            final int inCurrentConfigComp = Boolean.compare(other.inCurrentConfig, inCurrentConfig);
            if (inCurrentConfigComp != 0) {
                return inCurrentConfigComp;
            }
            // tiebreak by node id to have stable ordering
            return id.compareTo(other.id);
        }
    }
}
