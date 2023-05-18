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

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster.
 */
public class LegacyRemoteClusterSecuritySmokeIT extends ESRestTestCase {

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
            Request searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");
            Response response = client().performRequest(searchRequest);
            assertOK(response);
            ObjectPath responseObj = ObjectPath.createFromResponse(response);
            int totalHits = responseObj.evaluate("hits.total.value");
            assertThat(totalHits, equalTo(1));
        }
    }
}
