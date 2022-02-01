/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.components.controller.InstanceHasMaster;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GetHealthActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all")
            .build();
    }

    public void testGetHealth() throws Exception {
        GetHealthAction.Response response = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();
        assertEquals(cluster().getClusterName(), response.getClusterName().value());
        assertEquals(HealthStatus.GREEN, response.getStatus());

        assertEquals(2, response.getComponents().size());

        for (GetHealthAction.Component component : response.getComponents()) {
            assertEquals(HealthStatus.GREEN, component.status());
        }

        GetHealthAction.Component controller = response.getComponents()
            .stream()
            .filter(c -> c.name().equals("controller"))
            .findAny()
            .orElseThrow();
        assertEquals(1, controller.indicators().size());
        GetHealthAction.Indicator nodeDoesNotHaveMaster = controller.indicators().get(0);
        assertEquals(InstanceHasMaster.NAME, nodeDoesNotHaveMaster.getName());
        assertEquals(HealthStatus.GREEN, nodeDoesNotHaveMaster.getStatus());
        assertEquals(InstanceHasMaster.GREEN_SUMMARY, nodeDoesNotHaveMaster.getSummary());
    }

    public void testGetHealthInstanceNoMaster() throws Exception {
        Client client = internalCluster().coordOnlyNodeClient();

        final NetworkDisruption disruptionScheme = new NetworkDisruption(
            new NetworkDisruption.IsolateAllNodes(new HashSet<>(getNodes())),
            NetworkDisruption.DISCONNECT
        );

        internalCluster().setDisruptionScheme(disruptionScheme);
        disruptionScheme.startDisrupting();

        assertBusy(() -> {
            ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));

            GetHealthAction.Response response = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();
            assertEquals(HealthStatus.RED, response.getStatus());
            assertEquals(2, response.getComponents().size());
            GetHealthAction.Component controller = response.getComponents()
                .stream()
                .filter(c -> c.name().equals("controller"))
                .findAny()
                .orElseThrow();
            assertEquals(1, controller.indicators().size());
            InstanceHasMaster instanceHasMaster = (InstanceHasMaster) controller.indicators().get(0);
            assertEquals(InstanceHasMaster.NAME, instanceHasMaster.getName());
            assertEquals(HealthStatus.RED, instanceHasMaster.getStatus());
            assertEquals(InstanceHasMaster.RED_SUMMARY, instanceHasMaster.getSummary());
        });

        internalCluster().clearDisruptionScheme(true);
    }

    private List<String> getNodes() {
        return client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .collect(Collectors.toList());
    }
}
