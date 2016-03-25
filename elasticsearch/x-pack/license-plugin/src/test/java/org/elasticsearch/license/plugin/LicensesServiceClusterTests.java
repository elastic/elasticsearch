/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseResponse;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0, transportClientRatio = 0)
public class LicensesServiceClusterTests extends AbstractLicensesIntegrationTestCase {
    private final String[] PLUGINS = {EagerLicenseRegistrationPluginService.ID, LazyLicenseRegistrationPluginService.ID};

    @Override
    protected Settings transportClientSettings() {
        return super.transportClientSettings();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return nodeSettingsBuilder(nodeOrdinal).build();
    }

    private Settings.Builder nodeSettingsBuilder(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.data", true)
                // this setting is only used in tests
                .put("_trial_license_duration_in_seconds", 9)
                // this setting is only used in tests
                .put("_grace_duration_in_seconds", 9)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, EagerLicenseRegistrationConsumerPlugin.class, LazyLicenseRegistrationConsumerPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testClusterRestartWithLicense() throws Exception {
        wipeAllLicenses();

        int numNodes = randomIntBetween(1, 5);
        logger.info("--> starting {} node(s)", numNodes);
        for (int i = 0; i < numNodes; i++) {
            internalCluster().startNode();
        }
        ensureGreen();

        logger.info("--> put signed license");
        License license = generateAndPutLicenses();
        getAndCheckLicense(license);
        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> get and check signed license");
        getAndCheckLicense(license);

        logger.info("--> remove licenses");
        removeLicense();
        assertNoLicense();
        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();
        assertNoLicense();

        wipeAllLicenses();
    }

    public void testClusterRestartWhileEnabled() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, 5);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, 5);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for enabled");
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, 5);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, 5);
    }

    public void testClusterRestartWhileGrace() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, 10);
        assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, 10);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for grace_period");
        assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, 5);
        assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, 5);
    }

    public void testClusterRestartWhileExpired() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertEagerConsumerPluginNotification(LicenseState.DISABLED, 20);
        assertLazyConsumerPluginNotification(LicenseState.DISABLED, 20);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for disabled");
        assertEagerConsumerPluginNotification(LicenseState.DISABLED, 5);
        assertLazyConsumerPluginNotification(LicenseState.DISABLED, 5);
    }

    public void testClusterNotRecovered() throws Exception {
        logger.info("--> start one master out of two [recovery state]");
        internalCluster().startNode(nodeSettingsBuilder(0).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        logger.info("--> start second master out of two [recovered state]");
        internalCluster().startNode(nodeSettingsBuilder(1).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        assertLicenseesStateEnabled();
        assertConsumerPluginEnabledNotification(1);
    }

    public void testAtMostOnceTrialLicenseGeneration() throws Exception {
        wipeAllLicenses();
        logger.info("--> start one node [trial license should be generated & enabled]");
        internalCluster().startNode(nodeSettingsBuilder(0));
        assertLicenseesStateEnabled();
        assertConsumerPluginEnabledNotification(1);

        logger.info("--> start another node [trial license should be propagated from the old master not generated]");
        internalCluster().startNode(nodeSettings(1));
        assertLicenseesStateEnabled();
        assertConsumerPluginEnabledNotification(1);

        logger.info("--> check if multiple trial licenses are found for a id");
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getLicense(), not(LicensesMetaData.LICENSE_TOMBSTONE));

        wipeAllLicenses();
    }

    private void removeLicense() throws Exception {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        ensureGreen();
        License putLicenses = generateSignedLicense(TimeValue.timeValueMinutes(1));
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(cluster, PutLicenseAction.INSTANCE);
        putLicenseRequestBuilder.setLicense(putLicenses);
        DeleteLicenseResponse response = new DeleteLicenseRequestBuilder(cluster, DeleteLicenseAction.INSTANCE).get();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    private License generateAndPutLicenses() throws Exception {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        ensureGreen();
        License putLicenses = generateSignedLicense(TimeValue.timeValueMinutes(1));
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(cluster, PutLicenseAction.INSTANCE);
        putLicenseRequestBuilder.setLicense(putLicenses);
        final PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        return putLicenses;
    }

    private void assertNoLicense() {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        final GetLicenseResponse response = new GetLicenseRequestBuilder(cluster, GetLicenseAction.INSTANCE).get();
        assertThat(response.license(), nullValue());
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData, notNullValue());
        assertThat(licensesMetaData.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
    }

    private void getAndCheckLicense(License license) {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        final GetLicenseResponse response = new GetLicenseRequestBuilder(cluster, GetLicenseAction.INSTANCE).get();
        assertThat(response.license(), equalTo(license));
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData, notNullValue());
        assertThat(licensesMetaData.getLicense(), not(LicensesMetaData.LICENSE_TOMBSTONE));
    }

    private void assertLicenseesStateEnabled() throws Exception {
       for (String id : PLUGINS) {
           assertLicenseeState(id, LicenseState.ENABLED);
       }
    }

    private void assertConsumerPluginEnabledNotification(int timeoutInSec) throws InterruptedException {
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, timeoutInSec);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, timeoutInSec);
    }
}
