/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequest;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

public class LicenseRegistrationTests extends AbstractLicenseServiceTestCase {

    public void testTrialLicenseRequestOnEmptyLicenseState() throws Exception {
        setInitialState(null);
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee(
                "testTrialLicenseRequestOnEmptyLicenseState", logger);
        licensesService.start();
        licensesService.register(licensee);
        verify(transportService, times(1))
                .sendRequest(any(DiscoveryNode.class),
                        eq(LicensesService.REGISTER_TRIAL_LICENSE_ACTION_NAME),
                        any(TransportRequest.Empty.class), any(EmptyTransportResponseHandler.class));
        assertThat(licensee.statuses.size(), equalTo(0));
        licensesService.stop();
    }

    public void testNotificationOnRegistration() throws Exception {
        setInitialState(TestUtils.generateSignedLicense(TimeValue.timeValueHours(2)));
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee(
                "testNotificationOnRegistration", logger);
        licensesService.start();
        licensesService.register(licensee);
        assertThat(licensee.statuses.size(), equalTo(1));
        final LicenseState licenseState = licensee.statuses.get(0).getLicenseState();
        assertTrue(licenseState == LicenseState.ENABLED);
        licensesService.stop();
    }
}