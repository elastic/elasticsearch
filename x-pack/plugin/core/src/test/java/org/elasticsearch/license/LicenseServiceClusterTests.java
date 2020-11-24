/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0)
public class LicenseServiceClusterTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return nodeSettingsBuilder(nodeOrdinal).build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private Settings.Builder nodeSettingsBuilder(int nodeOrdinal) {
        return Settings.builder()
            .put(addRoles(super.nodeSettings(nodeOrdinal), Set.of(DiscoveryNodeRole.DATA_ROLE)))
            .put("resource.reload.interval.high", "500ms"); // for license mode file watcher
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class, Netty4Plugin.class);
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

    public void testCloudInternalLicense() throws Exception {
        wipeAllLicenses();

        int numNodes = randomIntBetween(1, 5);
        logger.info("--> starting {} node(s)", numNodes);
        internalCluster().startNodes(numNodes);
        ensureGreen();

        logger.info("--> put signed license");
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense("cloud_internal", License.VERSION_CURRENT, System.currentTimeMillis(),
                TimeValue.timeValueMinutes(1));
        putLicense(license);
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(license));
        assertOperationMode(License.OperationMode.PLATINUM);
        writeCloudInternalMode("gold");
        assertOperationMode(License.OperationMode.GOLD);
        writeCloudInternalMode("basic");
        assertOperationMode(License.OperationMode.BASIC);
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

    public void testClusterRestartWhileGrace() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        assertLicenseActive(true);
        putLicense(TestUtils.generateSignedLicense(TimeValue.timeValueMillis(0)));
        ensureGreen();
        assertLicenseActive(true);
        logger.info("--> restart node");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> await node for grace_period");
        assertLicenseActive(true);
    }

    public void testClusterRestartWhileExpired() throws Exception {
        wipeAllLicenses();
        internalCluster().startNode();
        ensureGreen();
        assertLicenseActive(true);
        putLicense(TestUtils.generateExpiredNonBasicLicense(System.currentTimeMillis() - LicenseService.GRACE_PERIOD_DURATION.getMillis()));
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
        assertThat(licensingClient.prepareGetLicense().get().license().version(), equalTo(License.VERSION_CURRENT)); //license updated
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

    private void writeCloudInternalMode(String mode) throws Exception {
        for (Environment environment : internalCluster().getDataOrMasterNodeInstances(Environment.class)) {
            Path licenseModePath = XPackPlugin.resolveConfigFile(environment, "license_mode");
            Files.createDirectories(licenseModePath.getParent());
            Files.write(licenseModePath, mode.getBytes(StandardCharsets.UTF_8));
        }
    }

}
