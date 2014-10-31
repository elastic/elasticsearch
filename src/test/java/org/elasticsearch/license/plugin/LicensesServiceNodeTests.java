/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.consumer.TestConsumerPlugin1;
import org.elasticsearch.license.plugin.consumer.TestPluginService1;
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
public class LicensesServiceNodeTests extends AbstractLicensesIntegrationTests {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(TestConsumerPlugin1.NAME + ".trial_license_duration_in_seconds", 5)
                .putArray("plugin.types", LicensePlugin.class.getName(), TestConsumerPlugin1.class.getName())
                .put(InternalNode.HTTP_ENABLED, true)
                .build();
    }

    @Test
    @TestLogging("_root:DEBUG")
    public void testPluginStatus() throws Exception {
        final Iterable<TestPluginService1> testPluginServices = internalCluster().getDataNodeInstances(TestPluginService1.class);
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (TestPluginService1 pluginService : testPluginServices) {
                    if (!pluginService.enabled()) {
                        return false;
                    }
                }
                return true;
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));

    }

}
