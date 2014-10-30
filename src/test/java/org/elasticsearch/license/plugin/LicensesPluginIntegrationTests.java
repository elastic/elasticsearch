/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10, numClientNodes = 0)
public class LicensesPluginIntegrationTests extends AbstractLicensesIntegrationTests {

    private final int trialLicenseDurationInSeconds = 2;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("test_consumer_plugin.trial_license_duration_in_seconds", trialLicenseDurationInSeconds)
                .put("plugin.types", LicensePlugin.class.getName() + "," + TestConsumerPlugin.class.getName())
                .build();
    }

    @After
    public void beforeTest() throws Exception {
        wipeAllLicenses();
    }

    @Test
    public void testTrialLicenseAndSignedLicenseNotification() throws Exception {
        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnableNotification(1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertConsumerPluginDisableNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(TestPluginService.FEATURE_NAME);

        logger.info(" --> put signed license");
        ESLicense license = generateSignedLicense(TestPluginService.FEATURE_NAME, TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));
        final PutLicenseResponse putLicenseResponse = new PutLicenseRequestBuilder(client().admin().cluster()).setLicense(Lists.newArrayList(license)).get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisableNotification(trialLicenseDurationInSeconds * 2);
        assertLicenseManagerDisabledFeatureFor(TestPluginService.FEATURE_NAME);
    }

    @Test
    public void testTrialLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnableNotification(1);

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000l);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisableNotification(trialLicenseDurationInSeconds);
        assertLicenseManagerDisabledFeatureFor(TestPluginService.FEATURE_NAME);
    }

    @Test
    public void testOverlappingTrialAndSignedLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginEnableNotification(1);

        logger.info(" --> put signed license while trial license is in effect");
        ESLicense license = generateSignedLicense(TestPluginService.FEATURE_NAME, TimeValue.timeValueSeconds(trialLicenseDurationInSeconds * 2));
        final PutLicenseResponse putLicenseResponse = new PutLicenseRequestBuilder(client().admin().cluster()).setLicense(Lists.newArrayList(license)).get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000l);

        logger.info(" --> check consumer is still enabled [signed license]");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginEnableNotification(1);
        assertLicenseManagerEnabledFeatureFor(TestPluginService.FEATURE_NAME);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginDisableNotification(trialLicenseDurationInSeconds * 2 * 2);
        assertLicenseManagerDisabledFeatureFor(TestPluginService.FEATURE_NAME);
    }


    private LicensesManagerService masterLicenseManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }
}
