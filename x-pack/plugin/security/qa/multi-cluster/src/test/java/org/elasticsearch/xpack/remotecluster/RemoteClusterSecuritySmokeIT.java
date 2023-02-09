/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster. The typical usage is to
 * conditionalize on whether the test is running against the fulfilling or the querying cluster.
 */
public class RemoteClusterSecuritySmokeIT extends ESRestTestCase {
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
        String token = basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()));
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
            Request searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");

            // Since authentication is not wired up on the FC side, getting a 401 response is sufficient
            // for testing connection (port, SSL) is working
            final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(e.getMessage(), containsString("missing authentication credentials"));

            // TODO: uncomment once authentication is wired up on the FC side
            // Response response = client().performRequest(searchRequest);
            // assertOK(response);
            // ObjectPath responseObj = ObjectPath.createFromResponse(response);
            // int totalHits = responseObj.evaluate("hits.total.value");
            // assertThat(totalHits, equalTo(1));
        }
    }

}
