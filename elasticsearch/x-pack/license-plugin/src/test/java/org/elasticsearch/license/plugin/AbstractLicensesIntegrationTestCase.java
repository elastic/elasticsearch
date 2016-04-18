/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.graph.Graph;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.TestPluginServiceBase;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.marvel.Monitoring;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Security;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public abstract class AbstractLicensesIntegrationTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Security.NAME), false)
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), false)
                .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false)
                .put(XPackPlugin.featureEnabledSetting(Graph.NAME), false)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.<Class<? extends Plugin>>singletonList(XPackPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    protected void wipeAllLicenses() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("delete licensing metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.removeCustom(LicensesMetaData.TYPE);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.error("error on metaData cleanup after test", t);
            }
        });
        latch.await();
    }

    protected void putLicense(TimeValue expiryDuration) throws Exception {
        License license1 = generateSignedLicense(expiryDuration);
        final PutLicenseResponse putLicenseResponse = new PutLicenseRequestBuilder(client().admin().cluster(),
                PutLicenseAction.INSTANCE).setLicense(license1).get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
    }

    protected void assertLicenseeState(final String id, final LicenseState state) throws InterruptedException {
        assertTrue("LicensesManagerService for licensee " + id + " should have status " + state.name(), awaitBusy(() -> {
            final InternalTestCluster clients = internalCluster();
            for (LicensesManagerService managerService : clients.getDataNodeInstances(LicensesManagerService.class)) {
                if (!managerService.licenseesWithState(state).contains(id)) {
                    return false;
                }
            }
            return true;
        }));
    }

    protected void assertLazyConsumerPluginNotification(final LicenseState state, int timeoutInSec) throws InterruptedException {
        final List<TestPluginServiceBase> consumerPluginServices = consumerLazyPluginServices();
        assertConsumerPluginNotification(consumerPluginServices, state, timeoutInSec);
    }

    protected void assertEagerConsumerPluginNotification(final LicenseState state, int timeoutInSec) throws InterruptedException {
        final List<TestPluginServiceBase> consumerPluginServices = consumerEagerPluginServices();
        assertConsumerPluginNotification(consumerPluginServices, state, timeoutInSec);
    }

    protected void assertConsumerPluginNotification(final List<TestPluginServiceBase> consumerPluginServices, final LicenseState state,
                                                    int timeoutInSec) throws InterruptedException {
        assertThat("At least one instance has to be present", consumerPluginServices.size(), greaterThan(0));
        boolean success = awaitBusy(() -> {
            for (TestPluginServiceBase pluginService : consumerPluginServices) {
                if (state != pluginService.state()) {
                    return false;
                }
            }
            return true;
        }, timeoutInSec + 1, TimeUnit.SECONDS);
        logger.debug("Notification assertion complete");
        assertThat(consumerPluginServices.get(0).getClass().getName() + " should have status " + state.name(), success, equalTo(true));
    }

    private List<TestPluginServiceBase> consumerLazyPluginServices() {
        final InternalTestCluster clients = internalCluster();
        List<TestPluginServiceBase> consumerPluginServices = new ArrayList<>();
        for (TestPluginServiceBase service : clients.getDataNodeInstances(LazyLicenseRegistrationPluginService.class)) {
            consumerPluginServices.add(service);
        }
        return consumerPluginServices;
    }

    private List<TestPluginServiceBase> consumerEagerPluginServices() {
        final InternalTestCluster clients = internalCluster();
        List<TestPluginServiceBase> consumerPluginServices = new ArrayList<>();
        for (TestPluginServiceBase service : clients.getDataNodeInstances(EagerLicenseRegistrationPluginService.class)) {
            consumerPluginServices.add(service);
        }
        return consumerPluginServices;
    }
}
