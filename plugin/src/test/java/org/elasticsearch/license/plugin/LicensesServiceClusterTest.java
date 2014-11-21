/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0, transportClientRatio = 0)
public class LicensesServiceClusterTest extends AbstractLicensesIntegrationTests {

    private final String[] FEATURES = {EagerLicenseRegistrationPluginService.FEATURE_NAME, LazyLicenseRegistrationPluginService.FEATURE_NAME};

    protected Settings transportClientSettings() {
        return super.transportClientSettings();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return nodeSettingsBuilder(nodeOrdinal).build();
    }

    private ImmutableSettings.Builder nodeSettingsBuilder(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("gateway.type", "local")
                .put("plugins.load_classpath_plugins", false)
                .put("node.data", true)
                .put("format", "json")
                .put(EagerLicenseRegistrationConsumerPlugin.NAME + ".trial_license_duration_in_seconds", 2)
                .put(LazyLicenseRegistrationConsumerPlugin.NAME + ".trial_license_duration_in_seconds", 2)
                .putArray("plugin.types", LicensePlugin.class.getName(), EagerLicenseRegistrationConsumerPlugin.class.getName(), LazyLicenseRegistrationConsumerPlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true);
    }

    @Test
    public void testClusterRestart() throws Exception {
        wipeAllLicenses();

        int numNodes = randomIntBetween(1, 5);
        logger.info("--> starting " + numNodes + " node(s)");
        for (int i = 0; i < numNodes; i++) {
            internalCluster().startNode();
        }
        ensureGreen();

        logger.info("--> put signed license");
        final List<License> licenses = generateAndPutLicenses();
        getAndCheckLicense(licenses);
        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> get and check signed license");
        getAndCheckLicense(licenses);

        wipeAllLicenses();
    }

    @Test
    public void testClusterNotRecovered() throws Exception {
        logger.info("--> start one master out of two [recovery state]");
        internalCluster().startNode(nodeSettingsBuilder(0).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        // license plugin should not be active when cluster is still recovering
        assertLicenseManagerFeatureDisabled();
        assertConsumerPluginDisabledNotification(1);

        logger.info("--> start second master out of two [recovered state]");
        internalCluster().startNode(nodeSettingsBuilder(1).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        assertLicenseManagerFeatureEnabled();
        assertConsumerPluginEnabledNotification(1);
    }

    @Test
    public void testAtMostOnceTrialLicenseGeneration() throws Exception {
        wipeAllLicenses();
        logger.info("--> start one node [trial license should be generated & enabled]");
        internalCluster().startNode(nodeSettingsBuilder(0));
        assertLicenseManagerFeatureEnabled();
        assertConsumerPluginEnabledNotification(1);

        logger.info("--> start another node [trial license should be propagated from the old master not generated]");
        internalCluster().startNode(nodeSettings(1));
        assertLicenseManagerFeatureEnabled();
        assertConsumerPluginEnabledNotification(1);

        logger.info("--> check if multiple trial licenses are found for a feature");
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getTrialLicenses().size(), equalTo(FEATURES.length));

        wipeAllLicenses();
    }

    private List<License> generateAndPutLicenses() throws Exception {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        List<License> putLicenses = new ArrayList<>(FEATURES.length);
        for (String feature : FEATURES) {
             putLicenses.add(generateSignedLicense(feature, TimeValue.timeValueMinutes(1)));
        }
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(cluster);
        putLicenseRequestBuilder.setLicense(putLicenses);
        ensureGreen();

        final PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();

        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        return putLicenses;
    }

    private void getAndCheckLicense(List<License> licenses) {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        final GetLicenseResponse response = new GetLicenseRequestBuilder(cluster).get();
        assertThat(response.licenses().size(), equalTo(licenses.size()));
        TestUtils.isSame(licenses, response.licenses());

        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData, notNullValue());
        assertThat(licensesMetaData.getTrialLicenses().size(), equalTo(2));
    }

    private void assertLicenseManagerFeatureEnabled() throws Exception {
       for (String feature : FEATURES) {
           assertLicenseManagerEnabledFeatureFor(feature);
       }
    }

    private void assertLicenseManagerFeatureDisabled() throws Exception {
        for (String feature : FEATURES) {
            assertLicenseManagerDisabledFeatureFor(feature);
        }
    }

    private void assertConsumerPluginEnabledNotification(int timeoutInSec) throws InterruptedException {
        assertEagerConsumerPluginEnableNotification(timeoutInSec);
        assertLazyConsumerPluginEnableNotification(timeoutInSec);
    }

    private void assertConsumerPluginDisabledNotification(int timeoutInSec) throws InterruptedException {
        assertEagerConsumerPluginDisableNotification(timeoutInSec);
        assertLazyConsumerPluginDisableNotification(timeoutInSec);
    }

}
