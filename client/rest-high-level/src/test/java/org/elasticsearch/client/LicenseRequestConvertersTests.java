/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.license.StartTrialRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.license.StartBasicRequest;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.license.DeleteLicenseRequest;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.PutLicenseRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.elasticsearch.client.RequestConvertersTests.setRandomMasterTimeout;
import static org.elasticsearch.client.RequestConvertersTests.setRandomTimeout;
import static org.hamcrest.CoreMatchers.is;


public class LicenseRequestConvertersTests extends ESTestCase {

    public void testGetLicense() {
        final boolean local = randomBoolean();
        final GetLicenseRequest getLicenseRequest = new GetLicenseRequest();
        getLicenseRequest.setLocal(local);
        final Map<String, String> expectedParams = new HashMap<>();
        if (local) {
            expectedParams.put("local", Boolean.TRUE.toString());
        }

        Request request = LicenseRequestConverters.getLicense(getLicenseRequest);
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_license"));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), is(nullValue()));
    }

    public void testPutLicense() {
        final boolean acknowledge = randomBoolean();
        final PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.setAcknowledge(acknowledge);
        final Map<String, String> expectedParams = new HashMap<>();
        if (acknowledge) {
            expectedParams.put("acknowledge", Boolean.TRUE.toString());
        }
        setRandomTimeout(putLicenseRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(putLicenseRequest, expectedParams);

        Request request = LicenseRequestConverters.putLicense(putLicenseRequest);
        assertThat(request.getMethod(), equalTo(HttpPut.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_license"));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), is(nullValue()));
    }

    public void testDeleteLicense() {
        final DeleteLicenseRequest deleteLicenseRequest = new DeleteLicenseRequest();
        final Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(deleteLicenseRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(deleteLicenseRequest, expectedParams);

        Request request = LicenseRequestConverters.deleteLicense(deleteLicenseRequest);
        assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_license"));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), is(nullValue()));
    }

    public void testStartTrial() {
        final boolean acknowledge = randomBoolean();
        final String licenseType = randomBoolean()
            ? randomAlphaOfLengthBetween(3, 10)
            : null;

        final Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("acknowledge", Boolean.toString(acknowledge));
        if (licenseType != null) {
            expectedParams.put("type", licenseType);
        }

        final StartTrialRequest hlrcRequest = new StartTrialRequest(acknowledge, licenseType);
        final Request restRequest = LicenseRequestConverters.startTrial(hlrcRequest);

        assertThat(restRequest.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(restRequest.getEndpoint(), equalTo("/_license/start_trial"));
        assertThat(restRequest.getParameters(), equalTo(expectedParams));
        assertThat(restRequest.getEntity(), nullValue());
    }

    public void testStartBasic() {
        final boolean acknowledge = randomBoolean();
        StartBasicRequest startBasicRequest = new StartBasicRequest(acknowledge);
        Map<String, String> expectedParams = new HashMap<>();
        if (acknowledge) {
            expectedParams.put("acknowledge", Boolean.TRUE.toString());
        }

        setRandomTimeout(startBasicRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(startBasicRequest, expectedParams);
        Request request = LicenseRequestConverters.startBasic(startBasicRequest);

        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_license/start_basic"));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), is(nullValue()));
    }

    public void testGetLicenseTrialStatus() {
        Request request = LicenseRequestConverters.getLicenseTrialStatus();
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_license/trial_status", request.getEndpoint());
        assertEquals(request.getParameters().size(), 0);
        assertNull(request.getEntity());
    }

    public void testGetLicenseBasicStatus() {
        Request request = LicenseRequestConverters.getLicenseBasicStatus();
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_license/basic_status", request.getEndpoint());
        assertEquals(request.getParameters().size(), 0);
        assertNull(request.getEntity());
    }
}
