/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
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
        try (RestClient restClient = restClient()) {
            try {
                logger.info("executing unauthorized request to /_xpack info");
                restClient.performRequest("GET", "/_xpack", Collections.emptyMap(), null);
                fail("request should have failed");
            } catch(ElasticsearchResponseException e) {
                assertThat(e.getElasticsearchResponse().getStatusLine().getStatusCode(), is(UNAUTHORIZED.getStatus()));
            }

            logger.info("executing authorized request to /_xpack infos");
            ElasticsearchResponse response = restClient.performRequest("GET", "/_xpack", Collections.emptyMap(), null,
                    new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                            basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                                    new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))));
            assertThat(response.getStatusLine().getStatusCode(), is(OK.getStatus()));
        }
    }
}
