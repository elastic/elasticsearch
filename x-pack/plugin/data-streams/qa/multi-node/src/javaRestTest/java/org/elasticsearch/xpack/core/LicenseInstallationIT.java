/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.license.License.VERSION_CURRENT;
import static org.elasticsearch.license.License.VERSION_ENTERPRISE;
import static org.elasticsearch.license.License.VERSION_NO_FEATURE_TYPE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that licenses can be installed start to finish via the REST API.
 */
public class LicenseInstallationIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Resets the license to a valid trial, no matter what state the license is in after each test.
     */
    @Before
    public void resetLicenseToTrial() throws Exception {
        License signedTrial = TestUtils.generateSignedLicense("trial", License.VERSION_CURRENT, -1, TimeValue.timeValueDays(14));
        Request putTrialRequest = new Request("PUT", "/_license");
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = signedTrial.toXContent(builder, ToXContent.EMPTY_PARAMS);
        putTrialRequest.setJsonEntity("{\"licenses\":[\n " + Strings.toString(builder) + "\n]}");
        assertBusy(() -> {
            Response putLicenseResponse = client().performRequest(putTrialRequest);
            logger.info("put trial license response when reseting license is [{}]", EntityUtils.toString(putLicenseResponse.getEntity()));
            assertOK(putLicenseResponse);
        });
        assertClusterUsingTrialLicense();
    }

    /**
     * Tests that we can install a valid, signed license via the REST API.
     */
    public void testInstallLicense() throws Exception {
        long futureExpiryDate = System.currentTimeMillis() + TimeValue.timeValueDays(randomIntBetween(1, 1000)).millis();
        License signedLicense = generateRandomLicense(UUID.randomUUID().toString(), futureExpiryDate);
        Request putLicenseRequest = createPutLicenseRequest(signedLicense);
        Response putLicenseResponse = client().performRequest(putLicenseRequest);
        assertOK(putLicenseResponse);
        Map<String, Object> responseMap = entityAsMap(putLicenseResponse);
        assertThat(responseMap.get("acknowledged").toString().toLowerCase(Locale.ROOT), equalTo("true"));
        assertThat(responseMap.get("license_status").toString().toLowerCase(Locale.ROOT), equalTo("valid"));

        Request getLicenseRequest = new Request("GET", "/_license");
        getLicenseRequest.addParameter("accept_enterprise", "true");
        Response getLicenseResponse = client().performRequest(getLicenseRequest);
        @SuppressWarnings("unchecked")
        Map<String, Object> innerMap = (Map<String, Object>) entityAsMap(getLicenseResponse).get("license");
        assertThat(innerMap.get("status"), equalTo("active"));
        assertThat(innerMap.get("type"), equalTo(signedLicense.type()));
        assertThat(innerMap.get("uid"), equalTo(signedLicense.uid()));
    }

    /**
     * Tests that we can try to install an expired license, and that it will be recognized as valid but expired.
     */
    public void testInstallExpiredLicense() throws Exception {
        // Use a very expired license to avoid any funkiness with e.g. grace periods
        long pastExpiryDate = System.currentTimeMillis() - TimeValue.timeValueDays(randomIntBetween(30, 1000)).millis();
        License signedLicense = generateRandomLicense(UUID.randomUUID().toString(), pastExpiryDate);
        Request putLicenseRequest = createPutLicenseRequest(signedLicense);
        Response putLicenseResponse = client().performRequest(putLicenseRequest);
        assertOK(putLicenseResponse);
        Map<String, Object> responseMap = entityAsMap(putLicenseResponse);
        assertThat(responseMap.get("acknowledged").toString().toLowerCase(Locale.ROOT), equalTo("true"));
        assertThat(responseMap.get("license_status").toString().toLowerCase(Locale.ROOT), equalTo("expired"));

        assertClusterUsingTrialLicense();
    }

    /**
     * Tests that license overrides work as expected - i.e. that the override date will be used instead of the date
     * in the license itself.
     */
    public void testInstallOverriddenExpiredLicense() throws Exception {
        long futureExpiryDate = System.currentTimeMillis() + TimeValue.timeValueDays(randomIntBetween(1, 1000)).millis();
        License signedLicense = generateRandomLicense("12345678-abcd-0000-0000-000000000000", futureExpiryDate);
        Request putLicenseRequest = createPutLicenseRequest(signedLicense);
        Response putLicenseResponse = client().performRequest(putLicenseRequest);
        assertOK(putLicenseResponse);
        Map<String, Object> responseMap = entityAsMap(putLicenseResponse);
        assertThat(responseMap.get("acknowledged").toString().toLowerCase(Locale.ROOT), equalTo("true"));
        assertThat(responseMap.get("license_status").toString().toLowerCase(Locale.ROOT), equalTo("expired"));

        assertClusterUsingTrialLicense();
    }

    private Request createPutLicenseRequest(License signedLicense) throws IOException {
        Request putLicenseRequest = new Request("PUT", "/_license");
        XContentBuilder xContent = JsonXContent.contentBuilder();
        xContent = signedLicense.toXContent(xContent, ToXContent.EMPTY_PARAMS);
        putLicenseRequest.setJsonEntity("{\"licenses\":[\n " + Strings.toString(xContent) + "\n]}");
        putLicenseRequest.addParameter("acknowledge", "true");
        return putLicenseRequest;
    }

    private License generateRandomLicense(String licenseId, long expiryDate) throws Exception {
        int version = randomIntBetween(VERSION_NO_FEATURE_TYPE, VERSION_CURRENT);
        License.LicenseType type = version < VERSION_ENTERPRISE
            ? randomValueOtherThan(License.LicenseType.ENTERPRISE, () -> randomFrom(LicenseService.ALLOWABLE_UPLOAD_TYPES))
            : randomFrom(LicenseService.ALLOWABLE_UPLOAD_TYPES);
        final License.Builder builder = License.builder()
            .uid(licenseId)
            .version(version)
            .expiryDate(expiryDate)
            .issueDate(randomLongBetween(0, System.currentTimeMillis()))
            .type(type)
            .issuedTo(this.getTestName() + " customer")
            .issuer(this.getTestName() + " issuer");
        if (type.equals(License.LicenseType.ENTERPRISE)) {
            builder.maxResourceUnits(randomIntBetween(1, 10000));
        } else {
            builder.maxNodes(randomIntBetween(1, 100));
        }
        License signedLicense = TestUtils.generateSignedLicense(builder);
        return signedLicense;
    }

    private void assertClusterUsingTrialLicense() throws Exception {
        Request getLicenseRequest = new Request("GET", "/_license");
        assertBusy(() -> {
            Response getLicenseResponse = client().performRequest(getLicenseRequest);
            @SuppressWarnings("unchecked")
            Map<String, Object> innerMap = (Map<String, Object>) entityAsMap(getLicenseResponse).get("license");
            assertThat("the cluster should be using a trial license", innerMap.get("type"), equalTo("trial"));
        });
    }
}
