/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.license.plugin.TestUtils.awaitNoBlock;
import static org.elasticsearch.license.plugin.TestUtils.awaitNoPendingTasks;
import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class LicensesAcknowledgementTests extends ESSingleNodeTestCase {
    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testAcknowledgment() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.start();
        String id = "testAcknowledgment";
        String[] acknowledgeMessages = new String[] {"message"};
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee(id, logger);
        licensee.setAcknowledgementMessages(acknowledgeMessages);
        awaitNoBlock(client());
        licensesService.register(licensee);
        awaitNoPendingTasks(client());
        // try installing a signed license
        License signedLicense = generateSignedLicense(TimeValue.timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        CountDownLatch latch = new CountDownLatch(1);
        // ensure acknowledgement message was part of the response
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID,
                Collections.singletonMap(id, acknowledgeMessages), latch));
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for a response to license registration");
        }
        awaitNoPendingTasks(client());
        assertThat(licensee.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensesService.getLicense(), not(signedLicense));

        latch = new CountDownLatch(1);
        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licensee.setAcknowledgementMessages(new String[0]);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID,
                Collections.<String, String[]>emptyMap(), latch));
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for a response to license registration");
        }
        awaitNoPendingTasks(client());
        assertThat(licensee.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensesService.getLicense(), equalTo(signedLicense));
        licensesService.stop();
    }

    public void testAcknowledgementMultipleLicensee() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.start();
        String id1 = "testAcknowledgementMultipleLicensee_1";
        String[] acknowledgeMessages1 = new String[] {"testAcknowledgementMultipleLicensee_1"};
        String id2 = "testAcknowledgementMultipleLicensee_2";
        String[] acknowledgeMessages2 = new String[] {"testAcknowledgementMultipleLicensee_2"};
        TestUtils.AssertingLicensee licensee1 = new TestUtils.AssertingLicensee(id1, logger);
        licensee1.setAcknowledgementMessages(acknowledgeMessages1);
        TestUtils.AssertingLicensee licensee2 = new TestUtils.AssertingLicensee(id2, logger);
        licensee2.setAcknowledgementMessages(acknowledgeMessages2);
        awaitNoBlock(client());
        licensesService.register(licensee1);
        licensesService.register(licensee2);
        awaitNoPendingTasks(client());
        // try installing a signed license
        License signedLicense = generateSignedLicense(TimeValue.timeValueHours(10));
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(signedLicense);
        CountDownLatch latch = new CountDownLatch(1);
        // ensure acknowledgement message was part of the response
        final HashMap<String, String[]> expectedMessages = new HashMap<>();
        expectedMessages.put(id1, acknowledgeMessages1);
        expectedMessages.put(id2, acknowledgeMessages2);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(false, LicensesStatus.VALID,
                expectedMessages, latch));
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for a response to license registration");
        }
        awaitNoPendingTasks(client());
        assertThat(licensee2.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee2.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensee1.acknowledgementRequested.size(), equalTo(1));
        assertThat(licensee1.acknowledgementRequested.get(0).v2(), equalTo(signedLicense));
        assertThat(licensesService.getLicense(), not(signedLicense));

        latch = new CountDownLatch(1);
        // try installing a signed license with acknowledgement
        putLicenseRequest = new PutLicenseRequest().license(signedLicense).acknowledge(true);
        // ensure license was installed and no acknowledgment message was returned
        licensee1.setAcknowledgementMessages(new String[0]);
        licensee2.setAcknowledgementMessages(new String[0]);
        licensesService.registerLicense(putLicenseRequest, new AssertingLicensesUpdateResponse(true, LicensesStatus.VALID,
                Collections.<String, String[]>emptyMap(), latch));
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for a response to license registration");
        }
        awaitNoPendingTasks(client());
        assertThat(licensesService.getLicense(), equalTo(signedLicense));
        licensesService.stop();
    }

    private static class AssertingLicensesUpdateResponse implements ActionListener<LicensesService.LicensesUpdateResponse> {
        private final boolean expectedAcknowledgement;
        private final LicensesStatus expectedStatus;
        private final Map<String, String[]> expectedAckMessages;
        private final CountDownLatch latch;

        public AssertingLicensesUpdateResponse(boolean expectedAcknowledgement, LicensesStatus expectedStatus,
                                               Map<String, String[]> expectedAckMessages, CountDownLatch latch) {
            this.expectedAcknowledgement = expectedAcknowledgement;
            this.expectedStatus = expectedStatus;
            this.expectedAckMessages = expectedAckMessages;
            this.latch = latch;
        }

        @Override
        public void onResponse(LicensesService.LicensesUpdateResponse licensesUpdateResponse) {
            assertThat(licensesUpdateResponse.isAcknowledged(), equalTo(expectedAcknowledgement));
            assertThat(licensesUpdateResponse.status(), equalTo(expectedStatus));
            assertThat(licensesUpdateResponse.acknowledgeMessages().size(), equalTo(expectedAckMessages.size()));
            for (Map.Entry<String, String[]> expectedEntry : expectedAckMessages.entrySet()) {
                Map<String, String[]> actual = licensesUpdateResponse.acknowledgeMessages();
                assertThat(actual.containsKey(expectedEntry.getKey()), equalTo(true));
                String[] actualMessages = actual.get(expectedEntry.getKey());
                assertThat(actualMessages, equalTo(expectedEntry.getValue()));
            }
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable throwable) {
            latch.countDown();
        }
    }
}
