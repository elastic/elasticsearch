/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BulkUpdateTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), randomBoolean())
                .build();
    }

    public void testThatBulkUpdateDoesNotLoseFields() {
        assertEquals(DocWriteResponse.Result.CREATED,
                client().prepareIndex("index1", "type").setSource("{\"test\": \"test\"}", XContentType.JSON).setId("1").get().getResult());
        GetResponse getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        // update with a new field
        assertEquals(DocWriteResponse.Result.UPDATED, internalCluster().transportClient().prepareUpdate("index1", "type", "1")
                .setDoc("{\"not test\": \"not test\"}", XContentType.JSON).get().getResult());
        getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));
        assertEquals("not test", getResponse.getSource().get("not test"));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        // do it in a bulk
        BulkResponse response = internalCluster().transportClient().prepareBulk().add(client().prepareUpdate("index1", "type", "1")
                .setDoc("{\"bulk updated\": \"bulk updated\"}", XContentType.JSON)).get();
        assertEquals(DocWriteResponse.Result.UPDATED, response.getItems()[0].getResponse().getResult());
        getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));
        assertEquals("not test", getResponse.getSource().get("not test"));
        assertEquals("bulk updated", getResponse.getSource().get("bulk updated"));
    }

    public void testThatBulkUpdateDoesNotLoseFieldsHttp() throws IOException {
        final String path = "/index1/type/1";
        final Header basicAuthHeader = new BasicHeader("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));

        StringEntity body = new StringEntity("{\"test\":\"test\"}", ContentType.APPLICATION_JSON);
        Response response = getRestClient().performRequest("PUT", path, Collections.emptyMap(), body, basicAuthHeader);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));

        response = getRestClient().performRequest("GET", path, basicAuthHeader);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"test\":\"test\""));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        //update with new field
        body = new StringEntity("{\"doc\": {\"not test\": \"not test\"}}", ContentType.APPLICATION_JSON);
        response = getRestClient().performRequest("POST", path + "/_update", Collections.emptyMap(), body, basicAuthHeader);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        response = getRestClient().performRequest("GET", path, basicAuthHeader);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("\"test\":\"test\""));
        assertThat(responseBody, containsString("\"not test\":\"not test\""));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        body = new StringEntity("{\"update\": {\"_index\": \"index1\", \"_type\": \"type\", \"_id\": \"1\"}}\n" +
                "{\"doc\": {\"bulk updated\":\"bulk updated\"}}\n", ContentType.APPLICATION_JSON);
        response = getRestClient().performRequest("POST", "/_bulk", Collections.emptyMap(), body, basicAuthHeader);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        response = getRestClient().performRequest("GET", path, basicAuthHeader);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("\"test\":\"test\""));
        assertThat(responseBody, containsString("\"not test\":\"not test\""));
        assertThat(responseBody, containsString("\"bulk updated\":\"bulk updated\""));
    }
}
