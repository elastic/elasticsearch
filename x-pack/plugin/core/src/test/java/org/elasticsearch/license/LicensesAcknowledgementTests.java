/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicensesAcknowledgementTests extends AbstractLicenseServiceTestCase {

    public void testAcknowledgment() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(TestUtils.generateSignedLicense("gold", timeValueHours(2)), licenseState, Settings.EMPTY);
        licenseService.start();
        // try installing a signed license
        long issueDate = System.currentTimeMillis() - TimeValue.timeValueHours(24 * 2).getMillis();
        License signedLicense = TestUtils.generateSignedLicense("trial", License.VERSION_CURRENT, issueDate, timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        // ensure acknowledgement message was part of the response
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID, true));
        assertThat(licenseService.getLicense(), not(signedLicense));
        verify(clusterService, times(0)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));

        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID, false));
        verify(clusterService, times(1)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }

    public void testRejectUpgradeToProductionWithoutTLS() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(TestUtils.generateSignedLicense("trial", timeValueHours(2)), licenseState, Settings.EMPTY);
        licenseService.start();
        // try installing a signed license
        License signedLicense = TestUtils.generateSignedLicense("platinum", timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        // ensure acknowledgement message was part of the response
        IllegalStateException ise = expectThrows(IllegalStateException.class, () ->
                licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID, true)));
        assertEquals("Cannot install a [PLATINUM] license unless TLS is configured or security is disabled", ise.getMessage());
    }

    public void testUpgradeToProductionWithoutTLSAndSecurityDisabled() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(TestUtils.generateSignedLicense("trial", timeValueHours(2)), licenseState, Settings.builder()
                .put("xpack.security.enabled", false).build());
        licenseService.start();
        // try installing a signed license
        License signedLicense = TestUtils.generateSignedLicense("platinum", timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID, true));
        assertThat(licenseService.getLicense(), not(signedLicense));
        verify(clusterService, times(1)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));

        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID, false));
        verify(clusterService, times(2)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }

    public void testUpgradeToProductionWithTLSAndSecurity() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(TestUtils.generateSignedLicense("trial", timeValueHours(2)), licenseState, Settings.builder()
                .put("xpack.security.enabled", true)
                .put("xpack.security.transport.ssl.enabled", true).build());
        licenseService.start();
        // try installing a signed license
        License signedLicense = TestUtils.generateSignedLicense("platinum", timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID, true));
        assertThat(licenseService.getLicense(), not(signedLicense));
        verify(clusterService, times(1)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));

        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licenseService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID, false));
        verify(clusterService, times(2)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }

    private static class AssertingLicensesUpdateResponse implements ActionListener<PutLicenseResponse> {
        private final boolean expectedAcknowledgement;
        private final LicensesStatus expectedStatus;
        private final boolean expectAckMessages;

        AssertingLicensesUpdateResponse(boolean expectedAcknowledgement, LicensesStatus expectedStatus,
                                               boolean expectAckMessages) {
            this.expectedAcknowledgement = expectedAcknowledgement;
            this.expectedStatus = expectedStatus;
            this.expectAckMessages = expectAckMessages;
        }

        @Override
        public void onResponse(PutLicenseResponse licensesUpdateResponse) {
            assertThat(licensesUpdateResponse.isAcknowledged(), equalTo(expectedAcknowledgement));
            assertThat(licensesUpdateResponse.status(), equalTo(expectedStatus));
            assertEquals(licensesUpdateResponse.acknowledgeMessages().isEmpty(), expectAckMessages == false);
        }

        @Override
        public void onFailure(Exception throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
