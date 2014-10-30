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
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0)
public class LicensesServiceClusterRestartTest extends AbstractLicensesIntegrationTests {

    @Override
    protected Settings transportClientSettings() {
        return super.transportClientSettings();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return nodeSettingsBuilder(nodeOrdinal).build();
    }

    private ImmutableSettings.Builder nodeSettingsBuilder(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("gateway.type", "local")
                .put("plugins.load_classpath_plugins", false)
                .put("node.data", true)
                .put("format", "json")
                .put("test_consumer_plugin.trial_license_duration_in_seconds", 5)
                .putArray("plugin.types", LicensePlugin.class.getName(), TestConsumerPlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true);
    }

    @Before
    public void beforeTest() throws Exception {
        wipeAllLicenses();
    }

    @Test
    public void testClusterRestart() throws Exception {
        logger.info("--> starting 1 node");
        internalCluster().startNode();
        ensureGreen();

        final List<ESLicense> esLicenses = generateAndPutLicense();
        getAndCheckLicense(esLicenses);
        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        getAndCheckLicense(esLicenses);
    }

    @Test
    public void testClusterNotRecovered() throws Exception {

        logger.info("--> start 1 node (should not recover)");
        internalCluster().startNode(nodeSettingsBuilder(0).put("gateway.recover_after_nodes", 3));
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        assertConsumerPluginEnableNotification(1);

        internalCluster().startNode(nodeSettingsBuilder(1).put("gateway.recover_after_nodes", 3));
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        assertConsumerPluginEnableNotification(1);

        internalCluster().startNode(nodeSettingsBuilder(2).put("gateway.recover_after_nodes", 3));
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        assertConsumerPluginEnableNotification(1);

        internalCluster().stopRandomNonMasterNode();
        // sleep for the entire trial license duration
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        assertConsumerPluginEnableNotification(1);

        Thread.sleep(5 * 1050l);
        internalCluster().startNode(nodeSettingsBuilder(3).put("gateway.recover_after_nodes", 3));
        assertLicenseManagerDisabledFeatureFor(TestPluginService.FEATURE_NAME);
        assertConsumerPluginDisableNotification(1);

    }

    private List<ESLicense> generateAndPutLicense() throws Exception {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        ESLicense license = generateSignedLicense("shield", TimeValue.timeValueMinutes(1));
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(cluster);
        final List<ESLicense> putLicenses = Arrays.asList(license);
        putLicenseRequestBuilder.setLicense(putLicenses);
        ensureGreen();

        final PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.execute().get();

        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        return putLicenses;
    }

    private void getAndCheckLicense(List<ESLicense> license) {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        final GetLicenseResponse response = new GetLicenseRequestBuilder(cluster).get();
        assertThat(response.licenses().size(), equalTo(1));
        TestUtils.isSame(license, response.licenses());
    }
}
