/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationConsumerPlugin;
import org.elasticsearch.license.plugin.consumer.LazyLicenseRegistrationPluginService;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@ClusterScope(scope = TEST, numDataNodes = 2, numClientNodes = 0)
public class LicensesPluginIntegrationTests extends AbstractLicensesIntegrationTestCase {
    private final boolean useEagerLicenseRegistrationPlugin = randomBoolean();

    private final int trialLicenseDurationInSeconds = 10;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // this setting is only used in tests
                .put("_trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        if (useEagerLicenseRegistrationPlugin) {
            return Arrays.asList(XPackPlugin.class, EagerLicenseRegistrationConsumerPlugin.class);
        } else {
            return Arrays.asList(XPackPlugin.class, LazyLicenseRegistrationConsumerPlugin.class);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @After
    public void afterTest() throws Exception {
        wipeAllLicenses();
        assertTrue(awaitBusy(() -> !clusterService().state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)));
    }

    public void testTrialLicenseAndSignedLicenseNotification() throws Exception {
        logger.info("using {} consumer plugin", useEagerLicenseRegistrationPlugin ? "eager" : "lazy");
        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(2);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.GRACE_PERIOD);

        assertLicenseeState(getCurrentFeatureName(), LicenseState.DISABLED);

        logger.info(" --> put signed license");
        putLicense(TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.GRACE_PERIOD);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.DISABLED);
    }

    public void testTrialLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.GRACE_PERIOD);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.DISABLED);
    }

    public void testOverlappingTrialAndSignedLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnabledNotification(1);

        logger.info(" --> put signed license while trial license is in effect");
        putLicense(TimeValue.timeValueSeconds(trialLicenseDurationInSeconds * 2));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000L);

        logger.info(" --> check consumer is still enabled [signed license]");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnabledNotification(1);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.ENABLED);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisabledNotification(trialLicenseDurationInSeconds * 2 * 2);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.GRACE_PERIOD);
        assertLicenseeState(getCurrentFeatureName(), LicenseState.DISABLED);
    }

    private String getCurrentFeatureName() {
        if (useEagerLicenseRegistrationPlugin) {
            return EagerLicenseRegistrationPluginService.ID;
        } else {
            return LazyLicenseRegistrationPluginService.ID;
        }
    }

    private void assertConsumerPluginEnabledNotification(int timeoutInSec) throws InterruptedException {
        if (useEagerLicenseRegistrationPlugin) {
            assertEagerConsumerPluginNotification(LicenseState.ENABLED, timeoutInSec);
        } else {
            assertLazyConsumerPluginNotification(LicenseState.ENABLED, timeoutInSec);
        }
    }

    private void assertConsumerPluginDisabledNotification(int timeoutInSec) throws InterruptedException {
        if (useEagerLicenseRegistrationPlugin) {
            assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, timeoutInSec);
        } else {
            assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, timeoutInSec);
        }
    }

}
