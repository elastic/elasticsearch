/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ShieldPluginTests extends ShieldIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("http.enabled", true) //This test requires HTTP
                .build();
    }

    public void testThatPluginIsLoaded() throws IOException {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            logger.info("executing unauthorized request to /_xpack info");
            HttpResponse response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport)
                    .method("GET")
                    .path("/_xpack")
                    .execute();
            assertThat(response.getStatusCode(), is(UNAUTHORIZED.getStatus()));

            logger.info("executing authorized request to /_xpack infos");
            response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport)
                    .method("GET")
                    .path("/_xpack")
                    .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                            new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())))
                    .execute();
            assertThat(response.getStatusCode(), is(OK.getStatus()));
        }
    }
}
