/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;

public class RequestsWithoutContentIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testIndexMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "POST" : "PUT", "/idx/_doc/123"))
        );
        assertResponseException(responseException, "request body is required");
    }

    public void testBulkMissingBody() throws IOException {
        Request request = new Request(randomBoolean() ? "POST" : "PUT", "/_bulk");
        request.setJsonEntity("");
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertResponseException(responseException, "request body is required");
    }

    public void testPutSettingsMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("PUT", "/_settings"))
        );
        assertResponseException(responseException, "request body is required");
    }

    public void testPutMappingsMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "POST" : "PUT", "/test_index/_mapping"))
        );
        assertResponseException(responseException, "request body is required");
    }

    public void testPutIndexTemplateMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "PUT" : "POST", "/_template/my_template"))
        );
        assertResponseException(responseException, "request body is required");
    }

    public void testMultiSearchMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "POST" : "GET", "/_msearch"))
        );
        assertResponseException(responseException, "request body or source parameter is required");
    }

    public void testPutPipelineMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("PUT", "/_ingest/pipeline/my_pipeline"))
        );
        assertResponseException(responseException, "request body or source parameter is required");
    }

    public void testSimulatePipelineMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "POST" : "GET", "/_ingest/pipeline/my_pipeline/_simulate"))
        );
        assertResponseException(responseException, "request body or source parameter is required");
    }

    public void testPutScriptMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomBoolean() ? "POST" : "PUT", "/_scripts/lang"))
        );
        assertResponseException(responseException, "request body is required");
    }

    private static void assertResponseException(ResponseException responseException, String message) {
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString(message));
    }
}
