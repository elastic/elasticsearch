/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, transportClientRatio = 0, numClientNodes = 0, numDataNodes = 0)
public class MonitoringPluginTests extends MonitoringIntegTestCase {

    @Override
    protected void startMonitoringService() {
        // do nothing as monitoring is sometime unbound
    }

    @Override
    protected void stopMonitoringService() {
        // do nothing as monitoring is sometime unbound
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    public void testMonitoringEnabled() {
        internalCluster().startNode(Settings.builder()
                .put(XPackSettings.MONITORING_ENABLED.getKey(), true)
                .build());
        assertPluginIsLoaded();
        assertServiceIsBound(MonitoringService.class);
    }

    public void testMonitoringDisabled() {
        internalCluster().startNode(Settings.builder()
                .put(XPackSettings.MONITORING_ENABLED.getKey(), false)
                .build());
        assertPluginIsLoaded();
        assertServiceIsNotBound(MonitoringService.class);
    }

    public void testMonitoringEnabledOnTribeNode() {
        internalCluster().startNode(Settings.builder()
                .put(XPackSettings.MONITORING_ENABLED.getKey(), true)
                .put("tribe.name", "t1")
                .build());
        assertPluginIsLoaded();
        assertServiceIsBound(MonitoringService.class);
    }

    public void testMonitoringDisabledOnTribeNode() {
        internalCluster().startNode(Settings.builder().put("tribe.name", "t1").build());
        assertPluginIsLoaded();
        assertServiceIsNotBound(MonitoringService.class);
    }

    private void assertPluginIsLoaded() {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
        for (NodeInfo nodeInfo : response.getNodes()) {
            assertNotNull(nodeInfo.getPlugins());

            boolean found = false;
            for (PluginInfo plugin : nodeInfo.getPlugins().getPluginInfos()) {
                assertNotNull(plugin);

                if (XPackPlugin.class.getName().equals(plugin.getName())) {
                    found = true;
                    break;
                }
            }
            assertThat("xpack plugin not found", found, equalTo(true));
        }
    }

    private void assertServiceIsBound(Class<?> klass) {
        try {
            Object binding = internalCluster().getDataNodeInstance(klass);
            assertNotNull(binding);
            assertTrue(klass.isInstance(binding));
        } catch (Exception e) {
            fail("no service bound for class " + klass.getSimpleName());
        }
    }

    private void assertServiceIsNotBound(Class<?> klass) {
        try {
            internalCluster().getDataNodeInstance(klass);
            fail("should have thrown an exception about missing implementation");
        } catch (Exception ce) {
            assertThat("message contains error about missing implementation: " + ce.getMessage(),
                    ce.getMessage().contains("Could not find a suitable constructor"), equalTo(true));
        }
    }
}
