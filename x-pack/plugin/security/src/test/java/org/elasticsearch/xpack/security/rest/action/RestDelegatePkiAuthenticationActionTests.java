/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Before;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RestDelegatePkiAuthenticationActionTests extends ESTestCase {

    private long timeInMillis;
    private Settings settings;
    private TestUtils.UpdatableLicenseState licenseState;
    private RestDelegatePkiAuthenticationAction action;

    @Before
    public void init() {
        timeInMillis = randomIntBetween(1, 1000);
        settings = Settings.builder().put("xpack.security.enabled", true).build();
        licenseState = new TestUtils.UpdatableLicenseState(settings, () -> timeInMillis);
        action = new RestDelegatePkiAuthenticationAction(settings, licenseState);
    }

    public void testPkiAvailableOnGoldOrAbove() {
        updateLicense(randomFrom(OperationMode.TRIAL, OperationMode.GOLD, OperationMode.PLATINUM, OperationMode.ENTERPRISE));
        assertNoRecordedFeatureUsage();
        assertThat(action.checkFeatureAvailable(new FakeRestRequest()), nullValue());
        assertFeatureUsageRecorded();
    }

    public void testPkiNotAvailableOnBasicOrStandard() {
        updateLicense(randomFrom(OperationMode.BASIC, OperationMode.STANDARD));
        assertNoRecordedFeatureUsage();
        assertThat(action.checkFeatureAvailable(new FakeRestRequest()), throwableWithMessage(containsString("[pki]")));
        assertFeatureUsageRecorded();
    }

    protected void assertNoRecordedFeatureUsage() {
        assertThat(licenseState.getLastUsed().get(XPackLicenseState.Feature.SECURITY_PKI_REALM), nullValue());
    }

    protected void assertFeatureUsageRecorded() {
        assertThat(licenseState.getLastUsed().get(XPackLicenseState.Feature.SECURITY_PKI_REALM), is(timeInMillis));
    }

    private void updateLicense(License.OperationMode mode) {
        this.licenseState.update(mode, true, null);
    }

}
