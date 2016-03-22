/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BulkUpdateTests extends ShieldIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put(XPackPlugin.featureEnabledSetting(Security.DLS_FLS_FEATURE), randomBoolean())
                .build();
    }

    public void testThatBulkUpdateDoesNotLoseFields() {
        assertThat(client().prepareIndex("index1", "type").setSource("{\"test\": \"test\"}").setId("1").get().isCreated(), is(true));
        GetResponse getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").setFields("test").get();
        assertThat(getResponse.getField("test").getValue(), equalTo("test"));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        // update with a new field
        boolean created = internalCluster().transportClient().prepareUpdate("index1", "type", "1").setDoc("{\"not test\": \"not test\"}")
                .get().isCreated();
        assertThat(created, is(false));
        getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").setFields("test", "not test").get();
        assertThat((String) getResponse.getField("test").getValue(), equalTo("test"));
        assertThat((String) getResponse.getField("not test").getValue(), equalTo("not test"));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        // do it in a bulk
        BulkResponse response = internalCluster().transportClient().prepareBulk().add(client().prepareUpdate("index1", "type", "1")
                .setDoc("{\"bulk updated\": \"bulk updated\"}")).get();
        assertThat(((UpdateResponse)response.getItems()[0].getResponse()).isCreated(), is(false));
        getResponse = internalCluster().transportClient().prepareGet("index1", "type", "1").
                setFields("test", "not test", "bulk updated").get();
        assertThat((String) getResponse.getField("test").getValue(), equalTo("test"));
        assertThat((String) getResponse.getField("not test").getValue(), equalTo("not test"));
        assertThat((String) getResponse.getField("bulk updated").getValue(), equalTo("bulk updated"));
    }

    public void testThatBulkUpdateDoesNotLoseFieldsHttp() throws IOException {
        final String path = "/index1/type/1";
        final String basicAuthHeader = UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()));

        httpClient().path(path).addHeader("Authorization", basicAuthHeader).method("PUT").body("{\"test\":\"test\"}").execute();
        HttpResponse response = httpClient().path(path).addHeader("Authorization", basicAuthHeader).method("GET").execute();
        assertThat(response.getBody(), containsString("\"test\":\"test\""));

        if (randomBoolean()) {
            flushAndRefresh();
        }

        //update with new field
        httpClient().path(path + "/_update").addHeader("Authorization", basicAuthHeader).method("POST").
                body("{\"doc\": {\"not test\": \"not test\"}}").execute();
        response = httpClient().path(path).addHeader("Authorization", basicAuthHeader).method("GET").execute();
        assertThat(response.getBody(), containsString("\"test\":\"test\""));
        assertThat(response.getBody(), containsString("\"not test\":\"not test\""));

        // this part is important. Without this, the document may be read from the translog which would bypass the bug where
        // FLS kicks in because the request can't be found and only returns meta fields
        flushAndRefresh();

        // update with bulk
        httpClient().path("/_bulk").addHeader("Authorization", basicAuthHeader).method("POST")
                .body("{\"update\": {\"_index\": \"index1\", \"_type\": \"type\", \"_id\": \"1\"}}\n{\"doc\": {\"bulk updated\":\"bulk " +
                        "updated\"}}\n")
                .execute();
        response = httpClient().path(path).addHeader("Authorization", basicAuthHeader).method("GET").execute();
        assertThat(response.getBody(), containsString("\"test\":\"test\""));
        assertThat(response.getBody(), containsString("\"not test\":\"not test\""));
        assertThat(response.getBody(), containsString("\"bulk updated\":\"bulk updated\""));
    }
}
