/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;

@ClusterScope(scope = SUITE, numDataNodes = 10)
public class LicensesPluginIntegrationTests extends ElasticsearchIntegrationTest {


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", LicensePlugin.class.getName() + "," + TestConsumerPlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    @Test
    public void testLicenseRegistration() throws Exception {
        LicensesManagerService managerService = licensesManagerService();
        assertTrue(managerService.enabledFeatures().contains(TestPluginService.FEATURE_NAME));
    }

    @Test
    public void testFeatureActivation() throws Exception {
        TestPluginService pluginService = consumerPluginService();
        assertTrue(pluginService.enabled());
    }

    private TestPluginService consumerPluginService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(TestPluginService.class, clients.getMasterName());
    }

    private LicensesManagerService licensesManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }
}
