/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NAME;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class InstanceHasMasterHealthIndicatorServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all")
            .build();
    }

    public void testGetHealthWhenMasterIsElected() throws Exception {
        var client = client();

        var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();

        assertThat(response.findComponent(CLUSTER_COORDINATION).findIndicator(NAME).status(), equalTo(GREEN));
    }

    public void testGetHealthWhenNoMaster() throws Exception {
        var client = internalCluster().coordOnlyNodeClient();

        var disruptionScheme = new NetworkDisruption(
            new NetworkDisruption.IsolateAllNodes(Set.of(internalCluster().getNodeNames())),
            NetworkDisruption.DISCONNECT
        );

        internalCluster().setDisruptionScheme(disruptionScheme);
        disruptionScheme.startDisrupting();

        try {
            assertBusy(() -> {
                ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));

                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();

                assertThat(response.findComponent(CLUSTER_COORDINATION).findIndicator(NAME).status(), equalTo(RED));
            });
        } finally {
            internalCluster().clearDisruptionScheme(true);
        }
    }
}
