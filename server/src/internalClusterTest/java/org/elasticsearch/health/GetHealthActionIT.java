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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.components.controller.ClusterCoordination;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

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

        for (HealthComponentResult component : response.getComponents()) {
            assertEquals(HealthStatus.GREEN, component.status());
        }

        HealthComponentResult controller = response.getComponents()
            .stream()
            .filter(c -> c.name().equals("cluster_coordination"))
            .findAny()
            .orElseThrow();
        assertEquals(1, controller.indicators().size());
        HealthIndicatorResult nodeDoesNotHaveMaster = controller.indicators().get(ClusterCoordination.INSTANCE_HAS_MASTER_NAME);
        assertEquals(ClusterCoordination.INSTANCE_HAS_MASTER_NAME, nodeDoesNotHaveMaster.name());
        assertEquals(HealthStatus.GREEN, nodeDoesNotHaveMaster.status());
        assertEquals(ClusterCoordination.INSTANCE_HAS_MASTER_GREEN_SUMMARY, nodeDoesNotHaveMaster.summary());
    }

    public void testGetHealthInstanceNoMaster() throws Exception {
        // builds the coordinating-only client before disrupting all nodes
        final Client client = internalCluster().coordOnlyNodeClient();

        final NetworkDisruption disruptionScheme = new NetworkDisruption(
            new NetworkDisruption.IsolateAllNodes(new HashSet<>(Arrays.asList(internalCluster().getNodeNames()))),
            NetworkDisruption.DISCONNECT
        );

        internalCluster().setDisruptionScheme(disruptionScheme);
        disruptionScheme.startDisrupting();

        try {
            assertBusy(() -> {
                ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));

                GetHealthAction.Response response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();
                assertEquals(HealthStatus.RED, response.getStatus());
                assertEquals(2, response.getComponents().size());
                HealthComponentResult controller = response.getComponents()
                    .stream()
                    .filter(c -> c.name().equals("cluster_coordination"))
                    .findAny()
                    .orElseThrow();
                assertEquals(1, controller.indicators().size());
                HealthIndicatorResult instanceHasMaster = controller.indicators().get(ClusterCoordination.INSTANCE_HAS_MASTER_NAME);
                assertEquals(ClusterCoordination.INSTANCE_HAS_MASTER_NAME, instanceHasMaster.name());
                assertEquals(HealthStatus.RED, instanceHasMaster.status());
                assertEquals(ClusterCoordination.INSTANCE_HAS_MASTER_RED_SUMMARY, instanceHasMaster.summary());
            });
        } finally {
            internalCluster().clearDisruptionScheme(true);
        }
    }
}
