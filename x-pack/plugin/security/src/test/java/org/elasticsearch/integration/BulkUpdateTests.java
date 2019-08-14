/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

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
        GetResponse getResponse = client().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        // update with a new field
        assertEquals(DocWriteResponse.Result.UPDATED, client().prepareUpdate("index1", "type", "1")
                .setDoc("{\"not test\": \"not test\"}", XContentType.JSON).get().getResult());
        getResponse = client().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));
        assertEquals("not test", getResponse.getSource().get("not test"));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        // do it in a bulk
        BulkResponse response = client().prepareBulk().add(client().prepareUpdate("index1", "type", "1")
                .setDoc("{\"bulk updated\": \"bulk updated\"}", XContentType.JSON)).get();
        assertEquals(DocWriteResponse.Result.UPDATED, response.getItems()[0].getResponse().getResult());
        getResponse = client().prepareGet("index1", "type", "1").get();
        assertEquals("test", getResponse.getSource().get("test"));
        assertEquals("not test", getResponse.getSource().get("not test"));
        assertEquals("bulk updated", getResponse.getSource().get("bulk updated"));
    }

    public void testThatBulkUpdateDoesNotLoseFieldsHttp() throws IOException {
        final String path = "/index1/type/1";
        final RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        final RequestOptions options = optionsBuilder.build();

        Request createRequest = new Request("PUT", path);
        createRequest.setOptions(options);
        createRequest.setJsonEntity("{\"test\":\"test\"}");
        getRestClient().performRequest(createRequest);

        Request getRequest = new Request("GET", path);
        getRequest.setOptions(options);
        assertThat(EntityUtils.toString(getRestClient().performRequest(getRequest).getEntity()), containsString("\"test\":\"test\""));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        //update with new field
        Request updateRequest = new Request("POST", path + "/_update");
        updateRequest.setOptions(options);
        updateRequest.setJsonEntity("{\"doc\": {\"not test\": \"not test\"}}");
        getRestClient().performRequest(updateRequest);

        String afterUpdate = EntityUtils.toString(getRestClient().performRequest(getRequest).getEntity());
        assertThat(afterUpdate, containsString("\"test\":\"test\""));
        assertThat(afterUpdate, containsString("\"not test\":\"not test\""));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setOptions(options);
        bulkRequest.setJsonEntity(
                "{\"update\": {\"_index\": \"index1\", \"_type\": \"type\", \"_id\": \"1\"}}\n" +
                "{\"doc\": {\"bulk updated\":\"bulk updated\"}}\n");
        getRestClient().performRequest(bulkRequest);

        String afterBulk = EntityUtils.toString(getRestClient().performRequest(getRequest).getEntity());
        assertThat(afterBulk, containsString("\"test\":\"test\""));
        assertThat(afterBulk, containsString("\"not test\":\"not test\""));
        assertThat(afterBulk, containsString("\"bulk updated\":\"bulk updated\""));
    }
}
