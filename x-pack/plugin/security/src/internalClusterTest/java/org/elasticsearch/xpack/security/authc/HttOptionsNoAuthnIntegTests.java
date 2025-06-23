/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class HttOptionsNoAuthnIntegTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // needed to test preflight requests
        builder.put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), "true")
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), "*");
        return builder.build();
    }

    public void testNoAuthnForResourceOptionsMethod() throws Exception {
        Request requestNoCredentials = new Request(
            "OPTIONS",
            randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush")
        );
        // no "Authorization" request header -> request is unauthenticated
        assertThat(requestNoCredentials.getOptions().getHeaders().isEmpty(), is(true));
        // WRONG "Authorization" request header
        Request requestWrongCredentials = new Request(
            "OPTIONS",
            randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush")
        );
        RequestOptions.Builder options = requestWrongCredentials.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, new SecureString("WRONG"))
        );
        requestWrongCredentials.setOptions(options);
        for (Request request : List.of(requestNoCredentials, requestWrongCredentials)) {
            Response response = getRestClient().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            assertThat(response.getHeader("Allow"), notNullValue());
            assertThat(response.getHeader("X-elastic-product"), is("Elasticsearch"));
            assertThat(response.getHeader("content-length"), is("0"));
        }
    }

    public void testNoAuthnForPreFlightRequest() throws Exception {
        Request requestNoCredentials = new Request(
            "OPTIONS",
            randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush")
        );
        RequestOptions.Builder options = requestNoCredentials.getOptions().toBuilder();
        options.addHeader(CorsHandler.ORIGIN, "google.com");
        options.addHeader(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, "GET");
        requestNoCredentials.setOptions(options);
        // no "Authorization" request header -> request is unauthenticated
        Request requestWrongCredentials = new Request(
            "OPTIONS",
            randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush")
        );
        options = requestWrongCredentials.getOptions().toBuilder();
        // WRONG "Authorization" request header
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, new SecureString("WRONG"))
        );
        options.addHeader(CorsHandler.ORIGIN, "google.com");
        options.addHeader(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, "GET");
        requestWrongCredentials.setOptions(options);
        for (Request request : List.of(requestWrongCredentials)) {
            Response response = getRestClient().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            assertThat(response.getHeader("content-length"), is("0"));
        }
    }

}
