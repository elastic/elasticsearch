/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class FleetSecretsSystemIndexIT extends ESRestTestCase {
    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testFleetSecretsCRUD() throws Exception {
        // post secret
        // TODO: this currently fails with "this request accesses system indices"
        final String secretJson = getPostSecretJson();
        Request postRequest = new Request("POST", "/_fleet/secrets/");
        postRequest.setJsonEntity(secretJson);
        System.out.println("----> request");
        System.out.println(postRequest);
        Response postResponse = client().performRequest(postRequest);
        System.out.println("----> response");
        System.out.println(postResponse);
        assertThat(postResponse.getStatusLine().getStatusCode(), is(201));

        // get secret
        // Request getRequest = new Request("GET", "/fleet/secrets/123"); // will need actual id returned by POST request
        // Response getResponse = client().performRequest(getRequest);
        // assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        // assertThat(EntityUtils.toString(getResponse.getEntity()), containsString(secretJson));

        // delete secret
        // Request deleteRequest = new Request("DELETE", "/fleet/secrets/123"); // will need actual id returned by POST request
        // Response deleteResponse = client().performRequest(deleteRequest);
        // assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testGetNonExistingSecret() throws Exception {
        // TODO: this currently fails with 400 Bad Request
        Request getRequest = new Request("GET", "/_fleet/secrets/123");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(404));
    }

    public void testDeleteNonExistingSecret() {
        // TODO: this currently passes but it's likely index not found
        Request deleteRequest = new Request("DELETE", "/_fleet/secrets/123");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(404));
    }

    private String getPostSecretJson() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("value", "test secret");
            }
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
