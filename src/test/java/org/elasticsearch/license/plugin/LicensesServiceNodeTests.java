/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = TEST, numDataNodes = 10, numClientNodes = 0)
public class LicensesServiceNodeTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return nodeSettings();
    }

    private Settings nodeSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("test_consumer_plugin.trial_license_duration_in_seconds", 10)
                .putArray("plugin.types", LicensePlugin.class.getName(), TestConsumerPlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings();
    }


    @Test
    @TestLogging("_root:DEBUG")
    public void testPluginStatus() throws Exception {
        final Iterable<TestPluginService> testPluginServices = internalCluster().getDataNodeInstances(TestPluginService.class);
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (TestPluginService pluginService : testPluginServices) {
                    if (!pluginService.enabled()) {
                        return false;
                    }
                }
                return true;
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));

    }

}
