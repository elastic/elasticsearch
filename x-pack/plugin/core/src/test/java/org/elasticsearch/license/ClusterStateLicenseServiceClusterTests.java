/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0)
public class ClusterStateLicenseServiceClusterTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return nodeSettingsBuilder(nodeOrdinal, otherSettings).build();
    }

    private Settings.Builder nodeSettingsBuilder(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(addRoles(super.nodeSettings(nodeOrdinal, otherSettings), Set.of(DiscoveryNodeRole.DATA_ROLE)));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class);
    }

    public void testClusterRestartWithLicense() throws Exception {
        wipeAllLicenses();

        int numNodes = randomIntBetween(1, 5);
        logger.info("--> starting {} node(s)", numNodes);
        internalCluster().startNodes(numNodes);
        ensureGreen();

        logger.info("--> put signed license");
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueMinutes(1));
        putLicense(license);
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(license));
        assertOperationMode(license.operationMode());

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();
        licensingClient = new LicensingClient(client());
        logger.info("--> get and check signed license");
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(license));
        logger.info("--> remove licenses");
        licensingClient.prepareDeleteLicense().get();
        assertOperationMode(License.OperationMode.BASIC);

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        licensingClient = new LicensingClient(client());
        ensureYellow();
        assertTrue(License.LicenseType.isBasic(licensingClient.prepareGetLicense().get().license().type()));
        assertOperationMode(License.OperationMode.BASIC);

        wipeAllLicenses();
    }

    public void testClusterRestartWhileEnabled() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertLicenseActive(true);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for enabled");
        assertLicenseActive(true);
    }

    public void testClusterRestartWhileExpired() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertLicenseActive(true);
        putLicense(TestUtils.generateExpiredNonBasicLicense(System.currentTimeMillis()));
        assertLicenseActive(false);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for disabled");
        assertLicenseActive(false);
    }

    public void testClusterRestartWithOldSignature() throws Exception {
        assumeFalse("Can't run in a FIPS JVM. We can't generate old licenses since PBEWithSHA1AndDESede is not available", inFipsJvm());
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertLicenseActive(true);
        putLicense(TestUtils.generateSignedLicenseOldSignature());
        LicensingClient licensingClient = new LicensingClient(client());
        assertThat(licensingClient.prepareGetLicense().get().license().version(), equalTo(License.VERSION_START_DATE));
        logger.info("--> restart node");
        internalCluster().fullRestart(); // restart so that license is updated
        ensureYellow();
        logger.info("--> await node for enabled");
        assertLicenseActive(true);
        licensingClient = new LicensingClient(client());
        assertThat(licensingClient.prepareGetLicense().get().license().version(), equalTo(License.VERSION_CURRENT)); // license updated
        internalCluster().fullRestart(); // restart once more and verify updated license is active
        ensureYellow();
        logger.info("--> await node for enabled");
        assertLicenseActive(true);
    }

    private void assertOperationMode(License.OperationMode operationMode) throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getDataNodeInstances(XPackLicenseState.class)) {
                if (licenseState.getOperationMode() == operationMode) {
                    return;
                }
            }
            fail("No data nodes found with operation mode [" + operationMode + "]");
        });
    }
}
