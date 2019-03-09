/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

public class SecurityPluginTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatPluginIsLoaded() throws IOException {
        try {
            logger.info("executing unauthorized request to /_xpack info");
            getRestClient().performRequest(new Request("GET", "/_xpack"));
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(UNAUTHORIZED.getStatus()));
        }

        logger.info("executing authorized request to /_xpack infos");

        Request request = new Request("GET", "/_xpack");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(OK.getStatus()));
    }
}
