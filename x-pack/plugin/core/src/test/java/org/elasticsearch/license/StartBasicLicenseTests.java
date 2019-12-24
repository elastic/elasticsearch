/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class StartBasicLicenseTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.data", true)
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, Netty4Plugin.class);
    }

    public void testStartBasicLicense() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense("trial",  License.VERSION_CURRENT, -1, TimeValue.timeValueHours(24));
        licensingClient.preparePutLicense(license).get();

        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("trial", getLicenseResponse.license().type());
        });

        // Testing that you can start a basic license when you have no license
        if (randomBoolean()) {
            licensingClient.prepareDeleteLicense().get();
            assertNull(licensingClient.prepareGetLicense().get().license());
        }

        RestClient restClient = getRestClient();
        Response response = restClient.performRequest(new Request("GET", "/_license/basic_status"));
        String body = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"eligible_to_start_basic\":true}", body);

        Request ackRequest = new Request("POST", "/_license/start_basic");
        ackRequest.addParameter("acknowledge", "true");
        Response response2 = restClient.performRequest(ackRequest);
        String body2 = Streams.copyToString(new InputStreamReader(response2.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response2.getStatusLine().getStatusCode());
        assertTrue(body2.contains("\"acknowledged\":true"));
        assertTrue(body2.contains("\"basic_was_started\":true"));

        assertBusy(() -> {
            GetLicenseResponse currentLicense = licensingClient.prepareGetLicense().get();
            assertEquals("basic", currentLicense.license().type());
        });

        long expirationMillis = licensingClient.prepareGetLicense().get().license().expiryDate();
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, expirationMillis);

        Response response3 = restClient.performRequest(new Request("GET", "/_license"));
        String body3 = Streams.copyToString(new InputStreamReader(response3.getEntity().getContent(), StandardCharsets.UTF_8));
        assertTrue(body3.contains("\"type\" : \"basic\""));
        assertFalse(body3.contains("expiry_date"));
        assertFalse(body3.contains("expiry_date_in_millis"));

        Response response4 = restClient.performRequest(new Request("GET", "/_license/basic_status"));
        String body4 = Streams.copyToString(new InputStreamReader(response4.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response3.getStatusLine().getStatusCode());
        assertEquals("{\"eligible_to_start_basic\":false}", body4);

        ResponseException ex = expectThrows(ResponseException.class,
                () -> restClient.performRequest(new Request("POST", "/_license/start_basic")));
        Response response5 = ex.getResponse();
        String body5 = Streams.copyToString(new InputStreamReader(response5.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(403, response5.getStatusLine().getStatusCode());
        assertTrue(body5.contains("\"basic_was_started\":false"));
        assertTrue(body5.contains("\"acknowledged\":true"));
        assertTrue(body5.contains("\"error_message\":\"Operation failed: Current license is basic.\""));
    }

    public void testUnacknowledgedStartBasicLicense() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense("trial",  License.VERSION_CURRENT, -1, TimeValue.timeValueHours(24));
        licensingClient.preparePutLicense(license).get();

        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("trial", getLicenseResponse.license().type());
        });

        Response response2 = getRestClient().performRequest(new Request("POST", "/_license/start_basic"));
        String body2 = Streams.copyToString(new InputStreamReader(response2.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response2.getStatusLine().getStatusCode());
        assertTrue(body2.contains("\"acknowledged\":false"));
        assertTrue(body2.contains("\"basic_was_started\":false"));
        assertTrue(body2.contains("\"error_message\":\"Operation failed: Needs acknowledgement.\""));
        assertTrue(body2.contains("\"message\":\"This license update requires acknowledgement. To acknowledge the license, " +
                "please read the following messages and call /start_basic again, this time with the \\\"acknowledge=true\\\""));
    }
}
