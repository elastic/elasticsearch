/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10, numClientNodes = 0)
public class LicensesPluginIntegrationTests extends ElasticsearchIntegrationTest {

    private final int trialLicenseDurationInSeconds = 5;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("test_consumer_plugin.trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .put("plugin.types", LicensePlugin.class.getName() + "," + TestConsumerPlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    @Test
    public void test() throws InterruptedException {
        // managerService should report feature to be enabled on all data nodes
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (LicensesManagerService managerService : licensesManagerServices()) {
                    if (!managerService.enabledFeatures().contains(TestPluginService.FEATURE_NAME)) {
                        return false;
                    }
                }
                return true;
            }
        }, 2, TimeUnit.SECONDS), equalTo(true));


        // consumer plugin service should return enabled on all data nodes
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (TestPluginService pluginService : consumerPluginServices()) {
                    if (!pluginService.enabled()) {
                        return false;
                    }
                }
                return true;
            }
        }, 2, TimeUnit.SECONDS), equalTo(true));


        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (TestPluginService pluginService : consumerPluginServices()) {
                    if(pluginService.enabled()) {
                        return false;
                    }

                }
                return true;
            }
        }, trialLicenseDurationInSeconds * 2, TimeUnit.SECONDS), equalTo(true));

    }

    private Iterable<TestPluginService> consumerPluginServices() {
        final InternalTestCluster clients = internalCluster();
        return clients.getDataNodeInstances(TestPluginService.class);
    }

    private Iterable<LicensesManagerService> licensesManagerServices() {
        final InternalTestCluster clients = internalCluster();
        return clients.getDataNodeInstances(LicensesManagerService.class);
    }

    private LicensesManagerService masterLicenseManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }
}
