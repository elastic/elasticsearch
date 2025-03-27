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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class FleetSecretsSystemIndexIT extends AbstractFleetIT {
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
        final String secretJson = getPostSecretJson();
        Request postRequest = new Request("POST", "/_fleet/secret/");
        postRequest.setJsonEntity(secretJson);
        Response postResponse = client().performRequest(postRequest);
        assertThat(postResponse.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> responseMap = getResponseMap(postResponse);
        assertThat(responseMap.size(), is(1));
        assertTrue(responseMap.containsKey("id"));
        final String id = responseMap.get("id").toString();

        // get secret
        Request getRequest = new Request("GET", "/_fleet/secret/" + id);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        responseMap = getResponseMap(getResponse);
        assertThat(responseMap.size(), is(2));
        assertTrue(responseMap.containsKey("id"));
        assertTrue(responseMap.containsKey("value"));
        assertThat(responseMap.get("value"), is("test secret"));

        // delete secret
        Request deleteRequest = new Request("DELETE", "/_fleet/secret/" + id);
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        responseMap = getResponseMap(deleteResponse);
        assertThat(responseMap.size(), is(1));
        assertTrue(responseMap.containsKey("deleted"));
        assertThat(responseMap.get("deleted"), is(true));
    }

    public void testPostInvalidSecretBody() throws Exception {
        Request postRequest = new Request("POST", "/_fleet/secret/");
        postRequest.setJsonEntity("""
            {"something":"else"}""");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(postRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(400));
    }

    public void testGetNonExistingSecret() {
        Request getRequest = new Request("GET", "/_fleet/secret/123");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(404));
    }

    public void testDeleteNonExistingSecret() {
        Request deleteRequest = new Request("DELETE", "/_fleet/secret/123");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
        Response deleteResponse = re.getResponse();
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(404));
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

    private Map<String, Object> getResponseMap(Response response) throws IOException {
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }
}
