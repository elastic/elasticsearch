/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicensesAcknowledgementTests extends AbstractLicenseServiceTestCase {

    public void testAcknowledgment() throws Exception {

        String id = "testAcknowledgment";
        String[] acknowledgeMessages = new String[] {"message"};
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee(id, logger);
        setInitialState(TestUtils.generateSignedLicense("trial", TimeValue.timeValueHours(2)), licensee);
        licensesService.start();
        licensee.setAcknowledgementMessages(acknowledgeMessages);
        // try installing a signed license
        License signedLicense = generateSignedLicense(TimeValue.timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        // ensure acknowledgement message was part of the response
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID,
                Collections.singletonMap(id, acknowledgeMessages)));
        assertThat(licensee.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensesService.getLicense(), not(signedLicense));

        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licensee.setAcknowledgementMessages(new String[0]);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID,
                Collections.<String, String[]>emptyMap()));
        verify(clusterService, times(1)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
        assertThat(licensee.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        licensesService.stop();
    }

    public void testAcknowledgementMultipleLicensee() throws Exception {
        String id1 = "testAcknowledgementMultipleLicensee_1";
        String[] acknowledgeMessages1 = new String[] {"testAcknowledgementMultipleLicensee_1"};
        String id2 = "testAcknowledgementMultipleLicensee_2";
        String[] acknowledgeMessages2 = new String[] {"testAcknowledgementMultipleLicensee_2"};
        TestUtils.AssertingLicensee licensee1 = new TestUtils.AssertingLicensee(id1, logger);
        licensee1.setAcknowledgementMessages(acknowledgeMessages1);
        TestUtils.AssertingLicensee licensee2 = new TestUtils.AssertingLicensee(id2, logger);
        licensee2.setAcknowledgementMessages(acknowledgeMessages2);
        setInitialState(TestUtils.generateSignedLicense("trial", TimeValue.timeValueHours(2)), licensee1, licensee2);
        licensesService.start();
        // try installing a signed license
        License signedLicense = generateSignedLicense(TimeValue.timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        // ensure acknowledgement message was part of the response
        final HashMap<String, String[]> expectedMessages = new HashMap<>();
        expectedMessages.put(id1, acknowledgeMessages1);
        expectedMessages.put(id2, acknowledgeMessages2);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID,
                expectedMessages));
        verify(clusterService, times(0)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
        assertThat(licensee2.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee2.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensee1.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee1.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensesService.getLicense(), not(signedLicense));

        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licensee1.setAcknowledgementMessages(new String[0]);
        licensee2.setAcknowledgementMessages(new String[0]);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID,
                Collections.<String, String[]>emptyMap()));
        verify(clusterService, times(1)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
        licensesService.stop();
    }

    private static class AssertingLicensesUpdateResponse implements ActionListener<PutLicenseResponse> {
        private final boolean expectedAcknowledgement;
        private final LicensesStatus expectedStatus;
        private final Map<String, String[]> expectedAckMessages;

        public AssertingLicensesUpdateResponse(boolean expectedAcknowledgement, LicensesStatus expectedStatus,
                                               Map<String, String[]> expectedAckMessages) {
            this.expectedAcknowledgement = expectedAcknowledgement;
            this.expectedStatus = expectedStatus;
            this.expectedAckMessages = expectedAckMessages;
        }

        @Override
        public void onResponse(PutLicenseResponse licensesUpdateResponse) {
            assertThat(licensesUpdateResponse.isAcknowledged(), equalTo(expectedAcknowledgement));
            assertThat(licensesUpdateResponse.status(), equalTo(expectedStatus));
            assertThat(licensesUpdateResponse.acknowledgeMessages().size(), equalTo(expectedAckMessages.size()));
            for (Map.Entry<String, String[]> expectedEntry : expectedAckMessages.entrySet()) {
                Map<String, String[]> actual = licensesUpdateResponse.acknowledgeMessages();
                assertThat(actual.containsKey(expectedEntry.getKey()), equalTo(true));
                String[] actualMessages = actual.get(expectedEntry.getKey());
                assertThat(actualMessages, equalTo(expectedEntry.getValue()));
            }
        }

        @Override
        public void onFailure(Exception throwable) {
        }
    }
}