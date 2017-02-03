/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.unit.TimeValue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicensesAcknowledgementTests extends AbstractLicenseServiceTestCase {

    public void testAcknowledgment() throws Exception {

        XPackLicenseState licenseState = new XPackLicenseState();
        setInitialState(TestUtils.generateSignedLicense("trial", TimeValue.timeValueHours(2)), licenseState);
        licenseService.start();
        // try installing a signed license
        License signedLicense = TestUtils.generateSignedLicense("basic", TimeValue.timeValueHours(10));
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
