/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.tribe.TribePlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, transportClientRatio = 0, numClientNodes = 0, numDataNodes = 0)
public class MonitoringPluginTests extends MonitoringIntegTestCase {

    public MonitoringPluginTests() throws Exception {
        super();
    }

    @Override
    protected void startMonitoringService() {
        // do nothing as monitoring is sometime unbound
    }

    @Override
    protected void stopMonitoringService() {
        // do nothing as monitoring is sometime unbound
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    public static class TribeAwareTestZenDiscoveryPlugin extends TestZenDiscovery.TestPlugin {

        public TribeAwareTestZenDiscoveryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Settings additionalSettings() {
            if (settings.getGroups("tribe", true).isEmpty()) {
                return super.additionalSettings();
            } else {
                return Settings.EMPTY;
            }
        }
    }

    public static class MockTribePlugin extends TribePlugin {

        public MockTribePlugin(Settings settings) {
            super(settings);
        }

        protected Function<Settings, Node> nodeBuilder(Path configPath) {
            return settings -> new MockNode(new Environment(settings, configPath), internalCluster().getPlugins());
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTribePlugin.class);
        plugins.add(TribeAwareTestZenDiscoveryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringService.INTERVAL.getKey(), "-1")
                .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
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

                if (LocalStateMonitoring.class.getName().equals(plugin.getName())) {
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
            // it should be bound, but directly as null
            assertNull(internalCluster().getDataNodeInstance(klass));
        } catch (Exception ce) {
            assertThat("message contains error about missing implementation: " + ce.getMessage(),
                    ce.getMessage().contains("Could not find a suitable constructor"), equalTo(true));
        }
    }
}
