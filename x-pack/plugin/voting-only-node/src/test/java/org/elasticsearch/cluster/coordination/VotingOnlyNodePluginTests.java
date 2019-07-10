/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class VotingOnlyNodePluginTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateVotingOnlyNodePlugin.class);
    }

    public void testRequireVotingOnlyNodeToBeMasterEligible() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> internalCluster().startNode(Settings.builder()
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true)
            .build()));
        assertThat(ise.getMessage(), containsString("voting-only node must be master-eligible"));
    }

    public void testVotingOnlyNodeStats() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        internalCluster().startNode(Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true));
        assertBusy(() -> assertThat(client().admin().cluster().prepareState().get().getState().getLastCommittedConfiguration().getNodeIds(),
            hasSize(3)));
        assertThat(client().admin().cluster().prepareClusterStats().get().getNodesStats().getCounts().getRoles().get(
            VotingOnlyNodePlugin.VOTING_ONLY_NODE_ROLE.roleName()).intValue(), equalTo(1));
        assertThat(client().admin().cluster().prepareNodesStats("voting_only:true").get().getNodes(), hasSize(1));
        assertThat(client().admin().cluster().prepareNodesStats("master:true", "voting_only:false").get().getNodes(), hasSize(2));
    }

    public void testPreferFullMasterOverVotingOnlyNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        internalCluster().startNode(Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true).build());
        internalCluster().startDataOnlyNodes(randomInt(2));
        assertBusy(() -> assertThat(
            client().admin().cluster().prepareState().get().getState().getLastCommittedConfiguration().getNodeIds().size(),
            equalTo(3)));
        final String originalMaster = internalCluster().getMasterName();

        internalCluster().stopCurrentMasterNode();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
        assertNotEquals(originalMaster, internalCluster().getMasterName());
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(client().admin().cluster().prepareState().get().getState().nodes().getMasterNode()),
            equalTo(false));
    }

    public void testBootstrapOnlyVotingOnlyNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true).build(),
            Settings.EMPTY, Settings.EMPTY);
        assertBusy(() -> assertThat(
            client().admin().cluster().prepareState().get().getState().getLastCommittedConfiguration().getNodeIds().size(),
            equalTo(3)));
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(client().admin().cluster().prepareState().get().getState().nodes().getMasterNode()),
            equalTo(false));
    }

    public void testBootstrapOnlySingleVotingOnlyNode() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode(Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true)
            .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build());
        internalCluster().startNode();
        assertBusy(() -> assertThat(client().admin().cluster().prepareState().get().getState().getNodes().getSize(), equalTo(2)));
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(client().admin().cluster().prepareState().get().getState().nodes().getMasterNode()),
            equalTo(false));
    }

    public void testVotingOnlyNodesCannotBeMasterWithoutFullMasterNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        internalCluster().startNodes(2, Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true).build());
        internalCluster().startDataOnlyNodes(randomInt(2));
        assertBusy(() -> assertThat(
            client().admin().cluster().prepareState().get().getState().getLastCommittedConfiguration().getNodeIds().size(),
            equalTo(3)));
        final String oldMasterId = client().admin().cluster().prepareState().get().getState().nodes().getMasterNodeId();

        internalCluster().stopCurrentMasterNode();

        expectThrows(MasterNotDiscoveredException.class, () ->
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet()
                .getState().nodes().getMasterNodeId(), nullValue()));

        // start a fresh full master node, which will be brought into the cluster as master by the voting-only nodes
        final String newMaster = internalCluster().startNode();
        assertEquals(newMaster, internalCluster().getMasterName());
        final String newMasterId = client().admin().cluster().prepareState().get().getState().nodes().getMasterNodeId();
        assertNotEquals(oldMasterId, newMasterId);
    }
}
