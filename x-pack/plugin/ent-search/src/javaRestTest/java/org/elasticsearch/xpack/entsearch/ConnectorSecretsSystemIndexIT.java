/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ConnectorSecretsSystemIndexIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-ent-search")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testConnectorSecretsCRUD() throws Exception {
        // post secret
        final String secretJson = getPostSecretJson();
        Request postRequest = new Request("POST", "/_connector/_secret/");
        postRequest.setJsonEntity(secretJson);
        Response postResponse = client().performRequest(postRequest);
        assertThat(postResponse.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> responseMap = getResponseMap(postResponse);
        assertThat(responseMap.size(), is(1));
        assertTrue(responseMap.containsKey("id"));
        final String id = responseMap.get("id").toString();

        // get secret
        Request getRequest = new Request("GET", "/_connector/_secret/" + id);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        responseMap = getResponseMap(getResponse);
        assertThat(responseMap.size(), is(2));
        assertTrue(responseMap.containsKey("id"));
        assertTrue(responseMap.containsKey("value"));
        assertThat(responseMap.get("value"), is("test secret"));
    }

    public void testPostInvalidSecretBody() throws Exception {
        Request postRequest = new Request("POST", "/_connector/_secret/");
        postRequest.setJsonEntity("""
            {"something":"else"}""");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(postRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(400));
    }

    public void testGetNonExistingSecret() {
        Request getRequest = new Request("GET", "/_connector/_secret/123");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
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

    private Map<String, Object> getResponseMap(Response response) throws IOException {
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
