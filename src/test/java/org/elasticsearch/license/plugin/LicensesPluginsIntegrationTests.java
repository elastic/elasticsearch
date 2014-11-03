/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.consumer.TestConsumerPlugin1;
import org.elasticsearch.license.plugin.consumer.TestConsumerPlugin2;
import org.elasticsearch.license.plugin.consumer.TestPluginService1;
import org.elasticsearch.license.plugin.consumer.TestPluginService2;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0)
public class LicensesPluginsIntegrationTests extends AbstractLicensesIntegrationTests {

    private final int trialLicenseDurationInSeconds = 2;
    
    private final String FEATURE_NAME_1 = TestPluginService1.FEATURE_NAME;
    private final String FEATURE_NAME_2 = TestPluginService2.FEATURE_NAME;

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(TestConsumerPlugin1.NAME + ".trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .put(TestConsumerPlugin2.NAME + ".trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .putArray("plugin.types", LicensePlugin.class.getName(), TestConsumerPlugin1.class.getName(), TestConsumerPlugin2.class.getName())
                .build();
    }

    private Settings nodeSettingsWithConsumerPlugin(int consumer1TrialLicenseDuration, int consumer2TrialLicenseDuration) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(0))
                .put(TestConsumerPlugin1.NAME + ".trial_license_duration_in_seconds", consumer1TrialLicenseDuration)
                .put(TestConsumerPlugin2.NAME + ".trial_license_duration_in_seconds", consumer2TrialLicenseDuration)
                .putArray("plugin.types", LicensePlugin.class.getName(), TestConsumerPlugin1.class.getName(), TestConsumerPlugin2.class.getName())
                .build();

    }

    @After
    public void beforeTest() throws Exception {
        wipeAllLicenses();
    }

    @Test
    public void testWithNoTrialLicense() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        String[] nodes = startNodesWithConsumerPlugins(nNodes, -1, -1);

        assertConsumerPlugin1DisableNotification(trialLicenseDurationInSeconds * 2);
        assertConsumerPlugin2DisableNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testOneTrialAndNonTrialConsumer() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        int consumer2TrialLicenseDuration = 2;
        String[] nodes = startNodesWithConsumerPlugins(nNodes, -1, consumer2TrialLicenseDuration);

        logger.info(" --> trial license generated for " + FEATURE_NAME_2 + " no trial license for " + FEATURE_NAME_1);
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPlugin1DisableNotification(1);
        assertConsumerPlugin2EnableNotification(1);

        logger.info(" --> put signed license for " + FEATURE_NAME_1);
        ESLicense license1 = generateSignedLicense(FEATURE_NAME_1, TimeValue.timeValueSeconds(consumer2TrialLicenseDuration));
        final PutLicenseResponse putLicenseResponse = new PutLicenseRequestBuilder(client().admin().cluster()).setLicense(Lists.newArrayList(license1)).get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        logger.info(" --> check that both " + FEATURE_NAME_1 + " and " + FEATURE_NAME_2 + " are enabled");
        assertConsumerPlugin1EnableNotification(1);
        assertConsumerPlugin2EnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPlugin1DisableNotification(consumer2TrialLicenseDuration * 2);
        assertConsumerPlugin2DisableNotification(consumer2TrialLicenseDuration * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testMultipleConsumerPlugins() throws Exception {

        int nNodes = randomIntBetween(2, 10);
        String[] nodes = startNodesWithConsumerPlugins(nNodes, trialLicenseDurationInSeconds, trialLicenseDurationInSeconds);

        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPlugin1EnableNotification(1);
        assertConsumerPlugin2EnableNotification(1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertConsumerPlugin1DisableNotification(trialLicenseDurationInSeconds * 2);
        assertConsumerPlugin2DisableNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> put signed license");
        putLicense(FEATURE_NAME_1, TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));
        putLicense(FEATURE_NAME_2, TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPlugin1EnableNotification(1);
        assertConsumerPlugin2EnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPlugin1DisableNotification(trialLicenseDurationInSeconds * 2);
        assertConsumerPlugin2DisableNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testRandomFeatureLicensesActions() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        int trialLicenseDuration1 = rarely() ? -1 : randomIntBetween(1, 2);
        int trialLicenseDuration2 = rarely() ? -1 : randomIntBetween(1, 2);

        String[] nodes = startNodesWithConsumerPlugins(nNodes, trialLicenseDuration1, trialLicenseDuration2);

        if (trialLicenseDuration1 != -1) {
            assertConsumerPlugin1EnableNotification(1);
            assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        } else {
            assertConsumerPlugin1DisableNotification(1);
            assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
            putLicense(FEATURE_NAME_1, TimeValue.timeValueMillis(300 * 2));
        }

        if (trialLicenseDuration2 != -1) {
            assertConsumerPlugin2EnableNotification(1);
            assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        } else {
            assertConsumerPlugin2DisableNotification(1);
            assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
            putLicense(FEATURE_NAME_2, TimeValue.timeValueMillis(300 * 2));
        }

        logger.info(" --> check license enabled notification");
        assertConsumerPlugin1EnableNotification(1);
        assertConsumerPlugin2EnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> check license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPlugin1DisableNotification(2 * 2);
        assertConsumerPlugin2DisableNotification(2 * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);

    }

    private String[] startNodesWithConsumerPlugins(int nNodes, int consumer1TrialLicenseDuration, int consumer2TrialLicenseDuration) {
        String[] nodes = new String[nNodes];
        for (int i = 0; i < nNodes; i++) {
            nodes[i] = internalCluster().startNode(nodeSettingsWithConsumerPlugin(consumer1TrialLicenseDuration, consumer2TrialLicenseDuration));
        }
        return nodes;
    }

    private void putLicense(String feature, TimeValue expiryDuration) throws Exception {
        ESLicense license1 = generateSignedLicense(feature, expiryDuration);
        final PutLicenseResponse putLicenseResponse = new PutLicenseRequestBuilder(client().admin().cluster()).setLicense(Lists.newArrayList(license1)).get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
    }
}
