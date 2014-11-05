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
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0, transportClientRatio = 0)
public class LicensesServiceClusterTest extends AbstractLicensesIntegrationTests {

    private final String FEATURE_NAME = EagerLicenseRegistrationPluginService.FEATURE_NAME;

    private final int trialLicenseDurationInSeconds = 2;

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
                .put(EagerLicenseRegistrationConsumerPlugin.NAME + ".trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .putArray("plugin.types", LicensePlugin.class.getName(), EagerLicenseRegistrationConsumerPlugin.class.getName())
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
        final List<ESLicense> esLicenses = generateAndPutLicense();
        getAndCheckLicense(esLicenses);
        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> get and check signed license");
        getAndCheckLicense(esLicenses);

        wipeAllLicenses();
    }

    @Test
    public void testClusterNotRecovered() throws Exception {
        logger.info("--> start one master out of two [recovery state]");
        internalCluster().startNode(nodeSettingsBuilder(0).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME);
        assertEagerConsumerPluginDisableNotification(1);

        logger.info("--> start second master out of two [recovered state]");
        internalCluster().startNode(nodeSettingsBuilder(1).put("discovery.zen.minimum_master_nodes", 2).put("node.master", true));
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME);
        assertEagerConsumerPluginEnableNotification(1);
    }

    private List<ESLicense> generateAndPutLicense() throws Exception {
        ClusterAdminClient cluster = internalCluster().client().admin().cluster();
        ESLicense license = generateSignedLicense(FEATURE_NAME, TimeValue.timeValueMinutes(1));
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
