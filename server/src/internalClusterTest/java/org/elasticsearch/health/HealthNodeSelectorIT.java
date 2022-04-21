/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class HealthNodeSelectorIT extends ESIntegTestCase {

    public void testDeselectingHealthNodeAboutToShutDown() throws Exception {
        // Set up a cluster with 2 health nodes that are not the elected master node
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNodes(1);
        List<String> healthNodes = internalCluster().startNodes(
            2,
            Settings.builder().put(onlyRoles(Set.of(DiscoveryNodeRole.HEALTH_ROLE))).build()
        );
        ensureStableCluster(3);

        // Wait until the health node selector task is assigned
        assertBusy(() -> assertThat(internalCluster().getHealthNodeName(), notNullValue()));
        String selectedHealthNode = internalCluster().getHealthNodeName();
        String otherHealthNode = healthNodes.stream().filter(node -> node.equals(selectedHealthNode) == false).findAny().get();

        // Shut down the selected health node and verify that the persistent task is unassigned
        internalCluster().stopNode(selectedHealthNode);
        assertBusy(() -> assertThat(internalCluster().getHealthNodeName(), nullValue()));

        // Ensure that the master node assigned the persistent task to the other health node
        assertBusy(() -> assertThat(internalCluster().getHealthNodeName(), equalTo(otherHealthNode)));
    }
}
