/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, transportClientRatio = 0, numClientNodes = 0, numDataNodes = 0)
public class MarvelPluginTests extends MarvelIntegTestCase {


    @Override
    protected void startCollection() {
        // do nothing as marvel is sometime unbound
    }

    @Override
    protected void stopCollection() {
        // do nothing as marvel is sometime unbound
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    public void testMarvelEnabled() {
        internalCluster().startNode(Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), true)
                .build());
        assertPluginIsLoaded();
        assertServiceIsBound(AgentService.class);
    }

    public void testMarvelDisabled() {
        internalCluster().startNode(Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), false)
                .build());
        assertPluginIsLoaded();
        assertServiceIsNotBound(AgentService.class);
    }

    public void testMarvelEnabledOnTribeNode() {
        internalCluster().startNode(Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), true)
                .put("tribe.name", "t1")
                .build());
        assertPluginIsLoaded();
        assertServiceIsBound(AgentService.class);
    }

    public void testMarvelDisabledOnTribeNode() {
        internalCluster().startNode(Settings.builder().put("tribe.name", "t1").build());
        assertPluginIsLoaded();
        assertServiceIsNotBound(AgentService.class);
    }

    private void assertPluginIsLoaded() {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
        for (NodeInfo nodeInfo : response) {
            assertNotNull(nodeInfo.getPlugins());

            boolean found = false;
            for (PluginInfo plugin : nodeInfo.getPlugins().getPluginInfos()) {
                assertNotNull(plugin);

                if (XPackPlugin.NAME.equals(plugin.getName())) {
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
            assertThat("message contains error about missing implemention: " + ce.getMessage(),
                    ce.getMessage().contains("No implementation"), equalTo(true));
        }
    }
}
