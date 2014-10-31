/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.licensor.ESLicenseSigner;
import org.elasticsearch.license.plugin.consumer.TestPluginService1;
import org.elasticsearch.license.plugin.consumer.TestPluginService2;
import org.elasticsearch.license.plugin.consumer.TestPluginServiceBase;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.license.AbstractLicensingTestBase.getTestPriKeyPath;
import static org.elasticsearch.license.AbstractLicensingTestBase.getTestPubKeyPath;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 */
public abstract class AbstractLicensesIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", LicensePlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    protected void wipeAllLicenses() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("delete licensing metadata", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.putCustom(LicensesMetaData.TYPE, null);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.error("error on metaData cleanup after test", t);
            }
        });
        latch.await();
    }

    public static ESLicense generateSignedLicense(String feature, TimeValue expiryDate) throws Exception {
        final ESLicense licenseSpec = ESLicense.builder()
                .uid(UUID.randomUUID().toString())
                .feature(feature)
                .expiryDate(System.currentTimeMillis() + expiryDate.getMillis())
                .issueDate(System.currentTimeMillis())
                .type("subscription")
                .subscriptionType("gold")
                .issuedTo("customer")
                .issuer("elasticsearch")
                .maxNodes(randomIntBetween(5, 100))
                .build();

        ESLicenseSigner signer = new ESLicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(licenseSpec);
    }

    protected void assertLicenseManagerEnabledFeatureFor(final String feature) throws InterruptedException {
        assertLicenseManagerStatusFor(feature, true);
    }

    protected void assertLicenseManagerDisabledFeatureFor(final String feature) throws InterruptedException {
        assertLicenseManagerStatusFor(feature, false);
    }

    protected void assertLicenseManagerStatusFor(final String feature, final boolean expectedEnabled) throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (LicensesManagerService managerService : licensesManagerServices()) {
                    if (expectedEnabled != managerService.enabledFeatures().contains(feature)) {
                        return false;
                    }
                }
                return true;
            }
        }, 2, TimeUnit.SECONDS), equalTo(true));
    }

    protected void assertConsumerPlugin1DisableNotification(int timeoutInSec) throws InterruptedException {
        assertConsumerPlugin1Notification(false, timeoutInSec);
    }

    protected void assertConsumerPlugin1EnableNotification(int timeoutInSec) throws InterruptedException {
        assertConsumerPlugin1Notification(true, timeoutInSec);
    }

    protected void assertConsumerPlugin2DisableNotification(int timeoutInSec) throws InterruptedException {
        assertConsumerPlugin2Notification(false, timeoutInSec);
    }

    protected void assertConsumerPlugin2EnableNotification(int timeoutInSec) throws InterruptedException {
        assertConsumerPlugin2Notification(true, timeoutInSec);
    }

    protected void assertConsumerPlugin2Notification(final boolean expectedEnabled, int timeoutInSec) throws InterruptedException {
        final Iterable<TestPluginServiceBase> consumerPluginServices = consumerPlugin2Services();
        assertConsumerPluginNotification(consumerPluginServices, expectedEnabled, timeoutInSec);
    }

    protected void assertConsumerPlugin1Notification(final boolean expectedEnabled, int timeoutInSec) throws InterruptedException {
        final Iterable<TestPluginServiceBase> consumerPluginServices = consumerPlugin1Services();
        assertConsumerPluginNotification(consumerPluginServices, expectedEnabled, timeoutInSec);
    }

    private void assertConsumerPluginNotification(final Iterable<TestPluginServiceBase> consumerPluginServices, final boolean expectedEnabled, int timeoutInSec) throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                for (TestPluginServiceBase pluginService : consumerPluginServices) {
                    if (expectedEnabled != pluginService.enabled()) {
                        return false;
                    }
                }
                return true;
            }
        }, timeoutInSec, TimeUnit.SECONDS), equalTo(true));

    }

    private Iterable<TestPluginServiceBase> consumerPlugin2Services() {
        final InternalTestCluster clients = internalCluster();
        List<TestPluginServiceBase> consumerPluginServices = new ArrayList<>();
        for (TestPluginServiceBase service : clients.getDataNodeInstances(TestPluginService2.class)) {
            consumerPluginServices.add(service);
        }
        return consumerPluginServices;
    }

    private Iterable<TestPluginServiceBase> consumerPlugin1Services() {
        final InternalTestCluster clients = internalCluster();
        List<TestPluginServiceBase> consumerPluginServices = new ArrayList<>();
        for (TestPluginServiceBase service : clients.getDataNodeInstances(TestPluginService1.class)) {
            consumerPluginServices.add(service);
        }
        return consumerPluginServices;
    }

    private Iterable<LicensesManagerService> licensesManagerServices() {
        final InternalTestCluster clients = internalCluster();
        return clients.getDataNodeInstances(LicensesManagerService.class);
    }
}
