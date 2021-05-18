/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FleetDataStreamIT extends ESRestTestCase {

    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        // Note that we are superuser here but DO NOT provide a product origin
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
            .build();
    }

    @Override protected Settings restAdminSettings() {
        // Note that we are both superuser here and provide a product origin
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
            .put(ThreadContext.PREFIX + ".X-elastic-product-origin", "fleet")
            .build();
    }

    public void testAliasWithSystemDataStream() throws Exception {
        Request initialDocResponse = new Request("POST", ".fleet-actions-results/_doc");
        initialDocResponse.setJsonEntity("{\"@timestamp\": 0}");
        Response response = adminClient().performRequest(initialDocResponse);
        assertOK(response);


        Request getAliasRequest = new Request("GET", "_alias/auditbeat-7.13.0");
        try {
            client().performRequest(getAliasRequest);
            fail("this request should not succeed, as it is looking for an alias that does not exist");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));
            assertThat(
                EntityUtils.toString(e.getResponse().getEntity()),
                not(containsString("use and access is reserved for system operations"))
            );
        }
    }
}
