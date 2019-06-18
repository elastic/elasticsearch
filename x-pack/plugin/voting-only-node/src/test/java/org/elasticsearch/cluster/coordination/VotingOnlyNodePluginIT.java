/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class VotingOnlyNodePluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(VotingOnlyNodePlugin.class);
    }

    public void testOneVotingOnlyNode() throws IOException {
        internalCluster().startNodes(2);
        final String votingOnlyNode
            = internalCluster().startNode(Settings.builder().put(VotingOnlyNodePlugin.VOTING_ONLY_NODE_SETTING.getKey(), true));

        internalCluster().stopCurrentMasterNode();

        assertNotEquals(votingOnlyNode, internalCluster().getMasterName());
    }
}
