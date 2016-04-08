/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.plugin.consumer.TestConsumerPluginBase;
import org.elasticsearch.license.plugin.consumer.TestPluginServiceBase;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

/**
 * Framework to test licensing plugin integration for existing/new consumer plugins
 * see {@link org.elasticsearch.license.plugin.LicensesEagerConsumerPluginIntegrationTests}
 * and {@link org.elasticsearch.license.plugin.LicensesLazyConsumerPluginIntegrationTests}
 * for example usage
 */
@ClusterScope(scope = TEST, numDataNodes = 2, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractLicensesConsumerPluginIntegrationTestCase extends AbstractLicensesIntegrationTestCase {
    protected final TestConsumerPluginBase consumerPlugin;

    public AbstractLicensesConsumerPluginIntegrationTestCase(TestConsumerPluginBase consumerPlugin) {
        this.consumerPlugin = consumerPlugin;
    }

    private final int trialLicenseDurationInSeconds = 20;

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
        return Arrays.asList(XPackPlugin.class, consumerPlugin.getClass());
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
        logger.info("using {} consumer plugin", consumerPlugin.getClass().getName());
        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 2);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.GRACE_PERIOD, trialLicenseDurationInSeconds * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.GRACE_PERIOD);
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.DISABLED, trialLicenseDurationInSeconds * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.DISABLED);

        logger.info(" --> put signed license");
        putLicense(TimeValue.timeValueSeconds(trialLicenseDurationInSeconds));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 1);
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.GRACE_PERIOD, trialLicenseDurationInSeconds * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.GRACE_PERIOD);
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.DISABLED, trialLicenseDurationInSeconds * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.DISABLED);
    }

    public void testTrialLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.GRACE_PERIOD, trialLicenseDurationInSeconds);
        assertLicenseeState(consumerPlugin.id(), LicenseState.GRACE_PERIOD);
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.DISABLED, trialLicenseDurationInSeconds);
        assertLicenseeState(consumerPlugin.id(), LicenseState.DISABLED);
    }

    public void testOverlappingTrialAndSignedLicenseNotification() throws Exception {
        logger.info(" --> check onEnabled for trial license");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 1);

        logger.info(" --> put signed license while trial license is in effect");
        putLicense(TimeValue.timeValueSeconds(trialLicenseDurationInSeconds * 2));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 1);
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);

        logger.info(" --> sleep for rest of trailLicense duration");
        Thread.sleep(trialLicenseDurationInSeconds * 1000L);

        logger.info(" --> check consumer is still enabled [signed license]");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.ENABLED, 1);
        assertLicenseeState(consumerPlugin.id(), LicenseState.ENABLED);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.GRACE_PERIOD, trialLicenseDurationInSeconds * 2 * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.GRACE_PERIOD);
        assertConsumerPluginNotification(consumerPluginServices(), LicenseState.DISABLED, trialLicenseDurationInSeconds * 2 * 2);
        assertLicenseeState(consumerPlugin.id(), LicenseState.DISABLED);
    }

    private List<TestPluginServiceBase> consumerPluginServices() {
        final InternalTestCluster clients = internalCluster();
        List<TestPluginServiceBase> consumerPluginServices = new ArrayList<>();
        for (TestPluginServiceBase service : clients.getDataNodeInstances(consumerPlugin.service())) {
            consumerPluginServices.add(service);
        }
        return consumerPluginServices;
    }
}
