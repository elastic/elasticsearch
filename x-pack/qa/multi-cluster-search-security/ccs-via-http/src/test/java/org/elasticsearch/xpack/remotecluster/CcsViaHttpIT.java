/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster.
 */
public class CcsViaHttpIT extends ESRestTestCase {

    private static final String USER = "test_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private boolean isFulfillingCluster() {
        return "fulfilling_cluster".equals(System.getProperty("tests.rest.suite"));
    }

    /**
     * This test really depends on the local build.gradle, which configures cross-cluster search using the `remote_cluster.*` settings.
     */
    public void testRemoteAccessPortFunctions() throws Exception {
        if (isFulfillingCluster()) {
            // Index some documents, so we can search them from the querying cluster
            Request indexDocRequest = new Request("POST", "/test_idx/_doc");
            indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
            Response response = client().performRequest(indexDocRequest);
            assertOK(response);

        } else {
            // Check that we can search the fulfilling cluster from the querying cluster

            // CCS via HTTP - directly send HTTP requests in TransportSearchAction
            Request searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");
            Response response = client().performRequest(searchRequest);
            assertOK(response);
            ObjectPath responseObj = ObjectPath.createFromResponse(response);
            int totalHits = responseObj.evaluate("hits.total.value");
            assertThat(totalHits, equalTo(1));

            // CCS via HTTP - intercept and send HTTP requests in SecurityServerTransportInterceptor
            Request searchRequest2 = new Request("GET", "/my_other_remote:test_idx/_search");
            searchRequest2.addParameter("ccs_minimize_roundtrips", String.valueOf(randomBoolean()));
            Response response2 = client().performRequest(searchRequest2);
            assertOK(response2);
            ObjectPath responseObj2 = ObjectPath.createFromResponse(response2);
            int totalHits2 = responseObj2.evaluate("hits.total.value");
            assertThat(totalHits2, equalTo(1));

            // PIT
            final Request openPitRequest = new Request("POST", "/my_other_remote:*/_pit?keep_alive=1h");
            final Response openPitResponse = client().performRequest(openPitRequest);
            assertOK(openPitResponse);
            final String pitId = ObjectPath.createFromResponse(openPitResponse).evaluate("id");
            assertThat(pitId, notNullValue());
            final Request pitSearchRequest = new Request("GET", "/_search");
            pitSearchRequest.setJsonEntity("""
                {
                  "pit": {
                    "id": "%s",
                    "keep_alive": "1h"
                  }
                }""".formatted(pitId));
            Response response3 = client().performRequest(searchRequest2);
            assertOK(response3);
            ObjectPath responseObj3 = ObjectPath.createFromResponse(response3);
            int totalHits3 = responseObj3.evaluate("hits.total.value");
            assertThat(totalHits3, equalTo(1));
        }
    }
}
