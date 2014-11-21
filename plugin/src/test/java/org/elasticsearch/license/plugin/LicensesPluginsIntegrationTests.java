/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0)
public class LicensesPluginsIntegrationTests extends AbstractLicensesIntegrationTests {

    private final String FEATURE_NAME_1 = EagerLicenseRegistrationPluginService.FEATURE_NAME;
    private final String FEATURE_NAME_2 = LazyLicenseRegistrationPluginService.FEATURE_NAME;

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .putArray("plugin.types", LicensePlugin.class.getName(), EagerLicenseRegistrationConsumerPlugin.class.getName(), LazyLicenseRegistrationConsumerPlugin.class.getName())
                .build();
    }

    private Settings nodeSettingsWithConsumerPlugin(int consumer1TrialLicenseDuration, int consumer2TrialLicenseDuration) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(0))
                .put(EagerLicenseRegistrationConsumerPlugin.NAME + ".trial_license_duration_in_seconds", consumer1TrialLicenseDuration)
                .put(LazyLicenseRegistrationConsumerPlugin.NAME + ".trial_license_duration_in_seconds", consumer2TrialLicenseDuration)
                .putArray("plugin.types", LicensePlugin.class.getName(), EagerLicenseRegistrationConsumerPlugin.class.getName(), LazyLicenseRegistrationConsumerPlugin.class.getName())
                .build();

    }

    @After
    public void afterTest() throws Exception {
        wipeAllLicenses();
    }

    @Test
    public void testWithNoTrialLicense() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        startNodesWithConsumerPlugins(nNodes, -1, -1);

        assertEagerConsumerPluginDisableNotification(1);
        assertLazyConsumerPluginDisableNotification(1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testOneTrialAndNonTrialConsumer() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        int consumer2TrialLicenseDuration = 5;
        startNodesWithConsumerPlugins(nNodes, -1, consumer2TrialLicenseDuration);

        logger.info(" --> trial license generated for " + FEATURE_NAME_2 + " no trial license for " + FEATURE_NAME_1);
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        // consumer plugin service should return enabled on all data nodes
        assertEagerConsumerPluginDisableNotification(1);
        assertLazyConsumerPluginEnableNotification(1);

        logger.info(" --> put signed license for " + FEATURE_NAME_1);
        putLicense(FEATURE_NAME_1, TimeValue.timeValueSeconds(consumer2TrialLicenseDuration));

        logger.info(" --> check that both " + FEATURE_NAME_1 + " and " + FEATURE_NAME_2 + " are enabled");
        assertEagerConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertEagerConsumerPluginDisableNotification(consumer2TrialLicenseDuration * 2);
        assertLazyConsumerPluginDisableNotification(consumer2TrialLicenseDuration * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testMultipleConsumerPlugins() throws Exception {

        int nNodes = randomIntBetween(2, 10);
        int consumer1TrialLicenseExpiry = 5;
        int consumer2TrialLicenseExpiry = 5;
        startNodesWithConsumerPlugins(nNodes, consumer1TrialLicenseExpiry, consumer2TrialLicenseExpiry);

        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        // consumer plugin service should return enabled on all data nodes
        assertEagerConsumerPluginEnableNotification(1);
        assertLazyConsumerPluginEnableNotification(1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertEagerConsumerPluginDisableNotification(consumer1TrialLicenseExpiry * 2);
        assertLazyConsumerPluginDisableNotification(consumer2TrialLicenseExpiry * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> put signed license");
        putLicense(FEATURE_NAME_1, TimeValue.timeValueSeconds(consumer1TrialLicenseExpiry));
        putLicense(FEATURE_NAME_2, TimeValue.timeValueSeconds(consumer2TrialLicenseExpiry));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertEagerConsumerPluginEnableNotification(1);
        assertLazyConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertEagerConsumerPluginDisableNotification(consumer1TrialLicenseExpiry * 2);
        assertLazyConsumerPluginDisableNotification(consumer2TrialLicenseExpiry * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
    }

    @Test
    public void testRandomFeatureLicensesActions() throws Exception {
        int nNodes = randomIntBetween(2, 10);
        int trialLicenseDuration1 = rarely() ? -1 : randomIntBetween(4, 6);
        int trialLicenseDuration2 = rarely() ? -1 : randomIntBetween(4, 6);

        startNodesWithConsumerPlugins(nNodes, trialLicenseDuration1, trialLicenseDuration2);

        if (trialLicenseDuration1 != -1) {
            assertEagerConsumerPluginEnableNotification(trialLicenseDuration1);
            assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        } else {
            assertEagerConsumerPluginDisableNotification(3 * 2);
            assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
            putLicense(FEATURE_NAME_1, TimeValue.timeValueMillis(500 * 2));
        }

        if (trialLicenseDuration2 != -1) {
            assertLazyConsumerPluginEnableNotification(trialLicenseDuration2);
            assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);
        } else {
            assertLazyConsumerPluginDisableNotification(3 * 2);
            assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);
            putLicense(FEATURE_NAME_2, TimeValue.timeValueMillis(500 * 2));
        }

        logger.info(" --> check license enabled notification");
        assertEagerConsumerPluginEnableNotification(1);
        assertLazyConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerEnabledFeatureFor(FEATURE_NAME_2);

        logger.info(" --> check license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertEagerConsumerPluginDisableNotification(3 * 2);
        assertLazyConsumerPluginDisableNotification(3 * 2);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_1);
        assertLicenseManagerDisabledFeatureFor(FEATURE_NAME_2);

    }

    private void startNodesWithConsumerPlugins(int nNodes, int consumer1TrialLicenseDuration, int consumer2TrialLicenseDuration) {
        for (int i = 0; i < nNodes; i++) {
            internalCluster().startNode(nodeSettingsWithConsumerPlugin(consumer1TrialLicenseDuration, consumer2TrialLicenseDuration));
        }
    }
}
