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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10, numClientNodes = 0)
public class LicensesPluginIntegrationTests extends AbstractLicensesIntegrationTests {

    private final boolean useEagerLicenseRegistrationPlugin = randomBoolean();

    private final int trialLicenseDurationInSeconds = 10;

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(((useEagerLicenseRegistrationPlugin) ? EagerLicenseRegistrationConsumerPlugin.NAME : LazyLicenseRegistrationConsumerPlugin.NAME)
                        + ".trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .putArray("plugin.types", LicensePlugin.class.getName(),
                        (useEagerLicenseRegistrationPlugin) ? EagerLicenseRegistrationConsumerPlugin.class.getName() : LazyLicenseRegistrationConsumerPlugin.class.getName())
                .build();
    }

    @After
    public void afterTest() throws Exception {
        wipeAllLicenses();
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return !clusterService().state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
            }
        }), equalTo(true));
    }

    @Test
    public void testTrialLicenseAndSignedLicenseNotification() throws Exception {
        logger.info("using " + ((useEagerLicenseRegistrationPlugin) ? "eager" : "lazy") + " consumer plugin");
        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(2);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(getCurrentFeatureName());

        logger.info(" --> put signed license");
        putLicense(getCurrentFeatureName(), TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(getCurrentFeatureName());
    }

    @Test
    public void testTrialLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(1);

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000l);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(getCurrentFeatureName());
    }

    @Test
    public void testOverlappingTrialAndSignedLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(1);

        logger.info(" --> put signed license while trial license is in effect");
        putLicense(getCurrentFeatureName(), TimeValue.timeValueSeconds(trialLicenseDurationInSeconds * 2));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000l);

        logger.info(" --> check consumer is still enabled [signed license]");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseManagerEnabledFeatureFor(getCurrentFeatureName());

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2 * 2);
        assertLicenseManagerDisabledFeatureFor(getCurrentFeatureName());
    }

    private String getCurrentFeatureName() {
        if (useEagerLicenseRegistrationPlugin) {
            return EagerLicenseRegistrationPluginService.FEATURE_NAME;
        } else {
            return LazyLicenseRegistrationPluginService.FEATURE_NAME;
        }
    }

    private void assertConsumerPluginEnabledNotification(int timeoutInSec) throws InterruptedException {
        if (useEagerLicenseRegistrationPlugin) {
            assertEagerConsumerPluginEnableNotification(timeoutInSec);
        } else {
            assertLazyConsumerPluginEnableNotification(timeoutInSec);
        }
    }

    private void assertConsumerPluginDisabledNotification(int timeoutInSec) throws InterruptedException {
        if (useEagerLicenseRegistrationPlugin) {
            assertEagerConsumerPluginDisableNotification(timeoutInSec);
        } else {
            assertLazyConsumerPluginDisableNotification(timeoutInSec);
        }
    }

}
