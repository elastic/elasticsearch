/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

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

    private class VotingOnlyNodeElectionStrategy implements ElectionStrategy {

        @Override
        public boolean isStateTransferOnly(DiscoveryNode discoveryNode) {
            return discoveryNode.isMasterNode() && discoveryNode.getRoles().contains(VOTING_ONLY_NODE_ROLE);
        }
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.singletonMap(VOTING_ONLY_ELECTION_TYPE, new VotingOnlyNodeElectionStrategy());
    }
}
