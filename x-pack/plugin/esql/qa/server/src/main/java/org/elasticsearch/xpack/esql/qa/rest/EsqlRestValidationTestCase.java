/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class EsqlRestValidationTestCase extends ESRestTestCase {

    private static final String indexName = "test_esql";
    private static final String aliasName = "alias-test_esql";

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Before
    public void prepareIndices() throws IOException {
        if (provisioningClient().performRequest(new Request("HEAD", "/" + indexName)).getStatusLine().getStatusCode() == 404) {
            var request = new Request("PUT", "/" + indexName);
            request.setJsonEntity("{\"mappings\": {\"properties\": {\"foo\":{\"type\":\"keyword\"}}}}");
            provisioningClient().performRequest(request);
        }
        assertOK(provisioningAdminClient().performRequest(new Request("POST", "/" + indexName + "/_refresh")));
    }

    @After
    public void wipeTestData() throws IOException {
        try {
            var response = provisioningAdminClient().performRequest(new Request("DELETE", "/" + indexName));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testInexistentIndexNameWithWildcard() throws IOException {
        for (String pattern : List.of("inexistent*", "inexistent1*,inexistent2*")) {
            assertEmpty(pattern);
        }
    }

    public void testInexistentIndexNameWithoutWildcard() {
        for (String pattern : List.of("inexistent", "inexistent1,inexistent2")) {
            assertError(pattern, 400, "Unknown index [" + clusterSpecificIndexName(pattern) + "]");
        }
    }

    public void testExistentIndexWithoutWildcard() throws IOException {
        for (String pattern : List.of(indexName + ",inexistent", "inexistent," + indexName)) {
            assertError(pattern, 404, "no such index [inexistent]");
        }
    }

    public void testExistentIndexWithWildcard() throws IOException {
        for (String pattern : List.of(indexName + ",inexistent*", indexName + "*,inexistent*", "inexistent*," + indexName)) {
            assertEmpty(pattern);
        }
    }

    public void testAlias() throws IOException {
        updateAliases("""
            {"actions":[{"add":{"index":"%s","alias":"%s"}}]}
            """.formatted(indexName, aliasName));

        for (String indexName : List.of(aliasName + ",inexistent", "inexistent," + aliasName)) {
            assertError(indexName, 404, "no such index [inexistent]");
        }
        for (String indexName : List.of(aliasName + ",inexistent*", aliasName + "*,inexistent*", "inexistent*," + aliasName)) {
            assertEmpty(indexName);
        }

        updateAliases("""
            {"actions":[{"remove":{"index":"%s","alias":"%s"}}]}
            """.formatted(indexName, aliasName));
    }

    protected String clusterSpecificIndexName(String indexName) {
        return indexName;
    }

    private void assertEmpty(String indexName) throws IOException {
        var response = client().performRequest(createRequest(indexName));
        Map<?, ?> usage = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), false);
        assertThat(usage.get("is_partial"), equalTo(false));
        assertThat((List<?>) usage.get("values"), hasSize(0));
    }

    private void assertError(String indexName, int statusCode, String errorMessage) {
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(createRequest(indexName)));
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        assertThat(exc.getMessage(), containsString("\"reason\" : \"" + errorMessage + "\""));
    }

    private Request createRequest(String indexName) throws IOException {
        final var request = new Request("POST", "/_query");
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder().startObject().field("query", "from " + clusterSpecificIndexName(indexName)).endObject()
            )
        );
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return request;
    }

    // Returned client is used to load the test data, either in the local cluster or a remote one (for
    // multi-clusters). The client()/adminClient() will always connect to the local cluster
    protected RestClient provisioningClient() throws IOException {
        return client();
    }

    protected RestClient provisioningAdminClient() throws IOException {
        return adminClient();
    }

    private void updateAliases(String update) throws IOException {
        var r = new Request("POST", "_aliases");
        r.setJsonEntity(update);
        assertOK(provisioningClient().performRequest(r));
    }
}
