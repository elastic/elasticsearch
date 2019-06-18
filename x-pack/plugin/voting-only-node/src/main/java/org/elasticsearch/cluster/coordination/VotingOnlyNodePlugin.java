/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VotingOnlyNodePlugin extends Plugin implements DiscoveryPlugin {

    private static final Logger logger = LogManager.getLogger(VotingOnlyNodePlugin.class);

    public static final Setting<Boolean> VOTING_ONLY_NODE_SETTING
        = Setting.boolSetting("node.voting_only", false, Setting.Property.NodeScope);

    private final Boolean isVotingOnlyNode;

    public VotingOnlyNodePlugin(Settings settings) {
        isVotingOnlyNode = VOTING_ONLY_NODE_SETTING.get(settings);
        if (isVotingOnlyNode && Node.NODE_MASTER_SETTING.get(settings) == false) {
            throw new IllegalStateException("voting-only node must be master-eligible");
        }
    }

    @Override
    public Set<DiscoveryNodeRole> getRoles() {
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

    private static boolean isTrueMasterNode(DiscoveryNode discoveryNode) {
        return discoveryNode.isMasterNode() && discoveryNode.getRoles().contains(VOTING_ONLY_NODE_ROLE) == false;
    }

    private class AvoidsVotingOnlyNodesElectionStrategy implements ElectionStrategy {
        @Override
        public boolean isAbdicationTarget(DiscoveryNode discoveryNode) {
            return isTrueMasterNode(discoveryNode);
        }

        @Override
        public boolean isGoodQuorum(Collection<DiscoveryNode> votingNodes) {
            return true;
        }

        @Override
        public boolean acceptPrevote(PreVoteResponse response, DiscoveryNode sender, ClusterState clusterState) {
            return true;
        }

        @Override
        public boolean shouldReceivePublication(DiscoveryNode destination) {
            return true;
        }

        @Override
        public ActionListener<PublishWithJoinResponse> wrapPublishResponseHandler(ActionListener<PublishWithJoinResponse> listener) {
            return listener;
        }
    }

    private class VotingOnlyNodeElectionStrategy implements ElectionStrategy {

        @Override
        public boolean isAbdicationTarget(DiscoveryNode discoveryNode) {
            assert false : "voting-only node cannot abdicate";
            return false;
        }


        @Override
        public boolean isGoodQuorum(Collection<DiscoveryNode> votingNodes) {
            return votingNodes.stream().anyMatch(VotingOnlyNodePlugin::isTrueMasterNode);
        }

        @Override
        public boolean acceptPrevote(PreVoteResponse response, DiscoveryNode sender, ClusterState clusterState) {
            if (sender.getRoles().contains(VOTING_ONLY_NODE_ROLE) == false
                && response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() == clusterState.version()) {
                logger.debug("{} ignoring {} from {} as it has higher voting priority", this, response, sender);
                return false;
            }

            return true;
        }

        @Override
        public boolean shouldReceivePublication(DiscoveryNode destination) {
            return isTrueMasterNode(destination);
        }

        @Override
        public ActionListener<PublishWithJoinResponse> wrapPublishResponseHandler(ActionListener<PublishWithJoinResponse> listener) {
            return new ActionListener<>() {
                @Override
                public void onResponse(PublishWithJoinResponse publishWithJoinResponse) {
                    listener.onFailure(new ElasticsearchException(
                        "ignoring successful publish response on voting-only node: " + publishWithJoinResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };
        }
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.singletonMap(VOTING_ONLY_ELECTION_TYPE,
            isVotingOnlyNode ? new VotingOnlyNodeElectionStrategy() : new AvoidsVotingOnlyNodesElectionStrategy());
    }
}
