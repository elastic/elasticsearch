/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    public void testClusterRestartWithLicense() throws Exception {
        int numNodes = randomIntBetween(1, 5);
        logger.info("--> starting {} node(s)", numNodes);
        internalCluster().startNodes(numNodes);
        ensureGreen();

        logger.info("--> put signed license");
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueMinutes(1));
        putLicense(license);
        assertThat(
            client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)).get().license(),
            equalTo(license)
        );
        assertOperationMode(license.operationMode());

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();
        logger.info("--> get and check signed license");
        assertThat(
            client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)).get().license(),
            equalTo(license)
        );
        logger.info("--> remove licenses");

        assertAcked(
            client().execute(TransportDeleteLicenseAction.TYPE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
                .get()
        );
        assertOperationMode(License.OperationMode.BASIC);

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();
        assertTrue(
            License.LicenseType.isBasic(
                client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)).get().license().type()
            )
        );
        assertOperationMode(License.OperationMode.BASIC);

        wipeAllLicenses();
    }

    public void testClusterRestartWhileEnabled() throws Exception {
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
        internalCluster().startNode();
        ensureGreen();
        assertLicenseActive(true);
        putLicense(TestUtils.generateSignedLicenseOldSignature());
        assertThat(
            client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)).get().license().version(),
            equalTo(License.VERSION_START_DATE)
        );
        logger.info("--> restart node");
        internalCluster().fullRestart(); // restart so that license is updated
        ensureYellow();
        logger.info("--> await node for enabled");
        assertLicenseActive(true);
        assertThat(
            client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)).get().license().version(),
            equalTo(License.VERSION_CURRENT)
        ); // license updated
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
