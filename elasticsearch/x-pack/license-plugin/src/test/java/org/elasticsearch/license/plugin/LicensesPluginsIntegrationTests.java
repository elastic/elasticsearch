/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0)
public class LicensesPluginsIntegrationTests extends AbstractLicensesIntegrationTestCase {

    private static final String ID_1 = EagerLicenseRegistrationPluginService.ID;
    private static final String ID_2 = LazyLicenseRegistrationPluginService.ID;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    private Settings nodeSettingsWithConsumerPlugin(int trialLicenseDuration) {
        return Settings.builder()
                .put(super.nodeSettings(0))
                // this setting is only used in tests
                .put("_trial_license_duration_in_seconds", trialLicenseDuration)
                .build();

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, EagerLicenseRegistrationConsumerPlugin.class, LazyLicenseRegistrationConsumerPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @After
    public void afterTest() throws Exception {
        wipeAllLicenses();
    }

    public void testMultipleConsumerPlugins() throws Exception {
        int nNodes = randomIntBetween(2, 3);
        int trialLicenseDurationInSec = 20;
        int signedLicenseDuration = 5;
        startNodesWithConsumerPlugins(nNodes, trialLicenseDurationInSec);

        logger.info(" --> trial license generated");
        // managerService should report feature to be enabled on all data nodes
        assertLicenseeState(ID_1, LicenseState.ENABLED);
        assertLicenseeState(ID_2, LicenseState.ENABLED);
        // consumer plugin service should return enabled on all data nodes
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, 1);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, 1);

        logger.info(" --> check trial license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired trial license)
        assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, trialLicenseDurationInSec * 2);
        assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, trialLicenseDurationInSec * 2);
        assertLicenseeState(ID_1, LicenseState.GRACE_PERIOD);
        assertLicenseeState(ID_2, LicenseState.GRACE_PERIOD);
        assertLicenseeState(ID_1, LicenseState.DISABLED);
        assertLicenseeState(ID_2, LicenseState.DISABLED);

        logger.info(" --> put signed license");
        putLicense(TimeValue.timeValueSeconds(signedLicenseDuration));

        logger.info(" --> check signed license enabled notification");
        // consumer plugin should notify onEnabled on all data nodes (signed license)
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, 1);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, 1);
        assertLicenseeState(ID_1, LicenseState.ENABLED);
        assertLicenseeState(ID_2, LicenseState.ENABLED);

        logger.info(" --> check signed license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, signedLicenseDuration * 2);
        assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, signedLicenseDuration * 2);
        assertLicenseeState(ID_1, LicenseState.GRACE_PERIOD);
        assertLicenseeState(ID_2, LicenseState.GRACE_PERIOD);

        assertEagerConsumerPluginNotification(LicenseState.DISABLED, 10 * 2);
        assertLazyConsumerPluginNotification(LicenseState.DISABLED, 10 * 2);
        assertLicenseeState(ID_1, LicenseState.DISABLED);
        assertLicenseeState(ID_2, LicenseState.DISABLED);
    }

    public void testRandomFeatureLicensesActions() throws Exception {
        int nNodes = randomIntBetween(2, 3);

        startNodesWithConsumerPlugins(nNodes, 10);

        logger.info(" --> check license enabled notification");
        assertEagerConsumerPluginNotification(LicenseState.ENABLED, 1);
        assertLazyConsumerPluginNotification(LicenseState.ENABLED, 1);
        assertLicenseeState(ID_1, LicenseState.ENABLED);
        assertLicenseeState(ID_2, LicenseState.ENABLED);

        logger.info(" --> check license expiry notification");
        // consumer plugin should notify onDisabled on all data nodes (expired signed license)
        assertEagerConsumerPluginNotification(LicenseState.GRACE_PERIOD, 10 * 2);
        assertLazyConsumerPluginNotification(LicenseState.GRACE_PERIOD, 10 * 2);
        assertLicenseeState(ID_1, LicenseState.GRACE_PERIOD);
        assertLicenseeState(ID_2, LicenseState.GRACE_PERIOD);
        assertEagerConsumerPluginNotification(LicenseState.DISABLED, 10 * 2);
        assertLazyConsumerPluginNotification(LicenseState.DISABLED, 10 * 2);
        assertLicenseeState(ID_1, LicenseState.DISABLED);
        assertLicenseeState(ID_2, LicenseState.DISABLED);
    }

    private void startNodesWithConsumerPlugins(int nNodes, int trialLicenseDuration) {
        for (int i = 0; i < nNodes; i++) {
            internalCluster().startNode(nodeSettingsWithConsumerPlugin(trialLicenseDuration));
        }
    }
}
