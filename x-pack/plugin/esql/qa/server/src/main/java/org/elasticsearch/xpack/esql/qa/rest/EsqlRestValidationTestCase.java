/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class EsqlRestValidationTestCase extends ESRestTestCase {

    private static final String indexName = "test_esql";
    private static final String aliasName = "alias-test_esql";
    protected static final String[] existentIndexWithWildcard = new String[] {
        indexName + ",inexistent*",
        indexName + "*,inexistent*",
        "inexistent*," + indexName };
    private static final String[] existentIndexWithoutWildcard = new String[] { indexName + ",inexistent", "inexistent," + indexName };
    protected static final String[] existentAliasWithWildcard = new String[] {
        aliasName + ",inexistent*",
        aliasName + "*,inexistent*",
        "inexistent*," + aliasName };
    private static final String[] existentAliasWithoutWildcard = new String[] { aliasName + ",inexistent", "inexistent," + aliasName };
    private static final String[] inexistentIndexNameWithWildcard = new String[] { "inexistent*", "inexistent1*,inexistent2*" };
    private static final String[] inexistentIndexNameWithoutWildcard = new String[] { "inexistent", "inexistent1,inexistent2" };
    private static final String createAlias = "{\"actions\":[{\"add\":{\"index\":\"" + indexName + "\",\"alias\":\"" + aliasName + "\"}}]}";
    private static final String removeAlias = "{\"actions\":[{\"remove\":{\"index\":\""
        + indexName
        + "\",\"alias\":\""
        + aliasName
        + "\"}}]}";

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

    private String getInexistentIndexErrorMessage() {
        return "\"reason\" : \"Found 1 problem\\nline 1:1: Unknown index ";
    }

    public void testInexistentIndexNameWithWildcard() throws IOException {
        assertErrorMessages(inexistentIndexNameWithWildcard, getInexistentIndexErrorMessage(), 400);
    }

    public void testInexistentIndexNameWithoutWildcard() throws IOException {
        assertErrorMessages(inexistentIndexNameWithoutWildcard, getInexistentIndexErrorMessage(), 400);
    }

    public void testExistentIndexWithoutWildcard() throws IOException {
        for (String indexName : existentIndexWithoutWildcard) {
            assertErrorMessage(indexName, "\"reason\" : \"no such index [inexistent]\"", 404);
        }
    }

    public void testExistentIndexWithWildcard() throws IOException {
        assertValidRequestOnIndices(existentIndexWithWildcard);
    }

    public void testAlias() throws IOException {
        createAlias();

        for (String indexName : existentAliasWithoutWildcard) {
            assertErrorMessage(indexName, "\"reason\" : \"no such index [inexistent]\"", 404);
        }
        assertValidRequestOnIndices(existentAliasWithWildcard);

        deleteAlias();
    }

    private void assertErrorMessages(String[] indices, String errorMessage, int statusCode) throws IOException {
        for (String indexName : indices) {
            assertErrorMessage(indexName, errorMessage + "[" + clusterSpecificIndexName(indexName) + "]", statusCode);
        }
    }

    protected String clusterSpecificIndexName(String indexName) {
        return indexName;
    }

    private void assertErrorMessage(String indexName, String errorMessage, int statusCode) throws IOException {
        var specificName = clusterSpecificIndexName(indexName);
        final var request = createRequest(specificName);
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        assertThat(exc.getMessage(), containsString(errorMessage));
    }

    private Request createRequest(String indexName) throws IOException {
        final var request = new Request("POST", "/_query");
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("query", "from " + indexName).endObject())
        );
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        request.setOptions(options);
        return request;
    }

    private void assertValidRequestOnIndices(String[] indices) throws IOException {
        for (String indexName : indices) {
            final var request = createRequest(clusterSpecificIndexName(indexName));
            Response response = client().performRequest(request);
            assertOK(response);
        }
    }

    // Returned client is used to load the test data, either in the local cluster or a remote one (for
    // multi-clusters). The client()/adminClient() will always connect to the local cluster
    protected RestClient provisioningClient() throws IOException {
        return client();
    }

    protected RestClient provisioningAdminClient() throws IOException {
        return adminClient();
    }

    private void createAlias() throws IOException {
        var r = new Request("POST", "_aliases");
        r.setJsonEntity(createAlias);
        assertOK(provisioningClient().performRequest(r));
    }

    private void deleteAlias() throws IOException {
        var r = new Request("POST", "/_aliases/");
        r.setJsonEntity(removeAlias);
        assertOK(provisioningAdminClient().performRequest(r));
    }
}
