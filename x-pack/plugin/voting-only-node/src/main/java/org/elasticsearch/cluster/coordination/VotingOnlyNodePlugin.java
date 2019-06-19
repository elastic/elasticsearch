/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.JoinVoteCollection;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VotingOnlyNodePlugin extends Plugin implements DiscoveryPlugin {

    public static final Setting<Boolean> VOTING_ONLY_NODE_SETTING
        = Setting.boolSetting("node.voting_only", false, Setting.Property.NodeScope);

    private final Settings settings;

    private final Boolean isVotingOnlyNode;

    public VotingOnlyNodePlugin(Settings settings) {
        this.settings = settings;
        isVotingOnlyNode = VOTING_ONLY_NODE_SETTING.get(settings);
    }

    @Override
    public Set<DiscoveryNodeRole> getRoles() {
        if (isVotingOnlyNode && Node.NODE_MASTER_SETTING.get(settings) == false) {
            throw new IllegalStateException("voting-only node must be master-eligible");
        }
        return Collections.singleton(VOTING_ONLY_NODE_ROLE);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(VOTING_ONLY_NODE_SETTING);
    }

    private static final String VOTING_ONLY_ELECTION_TYPE = "supports-voting-only";

    @Override
    public Settings additionalSettings() {
        return Settings.builder().put(DiscoveryModule.ELECTION_TYPE_SETTING.getKey(), VOTING_ONLY_ELECTION_TYPE).build();
    }

    static DiscoveryNodeRole VOTING_ONLY_NODE_ROLE = new DiscoveryNodeRole("voting-only", "v") {
        @Override
        protected Setting<Boolean> roleSetting() {
            return VOTING_ONLY_NODE_SETTING;
        }
    };

    public static boolean isVotingOnlyNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(VOTING_ONLY_NODE_ROLE);
    }

    public static boolean isFullMasterNode(DiscoveryNode discoveryNode) {
        return discoveryNode.isMasterNode() && discoveryNode.getRoles().contains(VOTING_ONLY_NODE_ROLE) == false;
    }

    private class VotingOnlyNodeElectionStrategy extends ElectionStrategy.DefaultElectionStrategy {

        @Override
        public boolean isStateTransferOnly(DiscoveryNode discoveryNode) {
            return discoveryNode.isMasterNode() && discoveryNode.getRoles().contains(VOTING_ONLY_NODE_ROLE);
        }

        @Override
        public boolean isElectionQuorum(DiscoveryNode localNode, long localCurrentTerm, long localAcceptedTerm, long localAcceptedVersion,
                                        VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                                        VoteCollection joinVotes) {
            // if local node is voting only, have additional checks on election quorum definition
            if (isVotingOnlyNode(localNode)) {
                // if all votes are from voting only nodes, do not elect as master (no need to transfer state)
                if (joinVotes.nodes().stream().filter(DiscoveryNode::isMasterNode).allMatch(VotingOnlyNodePlugin::isVotingOnlyNode)) {
                    return false;
                }
                // if there's a vote from a full master node with same last accepted term and version, that node should become master
                // instead, so we should stand down
                if (joinVotes instanceof JoinVoteCollection &&
                    ((JoinVoteCollection) joinVotes).getJoins().stream().anyMatch(join -> isFullMasterNode(join.getSourceNode()) &&
                        join.getLastAcceptedTerm() == localAcceptedTerm &&
                        join.getLastAcceptedVersion() == localAcceptedVersion)) {
                    return false;
                }
            }
            // fall back to default election quorum definition
            return super.isElectionQuorum(localNode, localCurrentTerm, localAcceptedTerm, localAcceptedVersion,
                lastCommittedConfiguration, lastAcceptedConfiguration, joinVotes);
        }
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.singletonMap(VOTING_ONLY_ELECTION_TYPE, new VotingOnlyNodeElectionStrategy());
    }
}
