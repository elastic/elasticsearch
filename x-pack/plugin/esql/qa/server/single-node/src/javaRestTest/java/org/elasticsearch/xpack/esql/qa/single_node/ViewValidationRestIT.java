/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Creating a view reports what is wrong with its query as warnings. A query that cannot be parsed is still rejected.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ViewValidationRestIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testValidViewHasNoWarnings() throws IOException {
        createIndex("view-validation-index", null, "\"properties\": {\"k\": {\"type\": \"keyword\"}}");
        assertThat(putView("view-validation-valid", "FROM view-validation-index | KEEP k", true).getWarnings(), empty());
    }

    public void testValidationCanBeSkipped() throws IOException {
        assertThat(putView("view-validation-skipped", "FROM no-such-index-for-view", false).getWarnings(), empty());
    }

    public void testParseErrorStillFailsTheRequest() {
        ResponseException e = expectThrows(
            ResponseException.class,
            containsString("mismatched input '<EOF>'"),
            () -> putView("view-validation-unparsable", "FROM view-validation-index |", true)
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testUnknownIndexEmitsWarning() throws IOException {
        Response response = putView("view-validation-unknown-index", "FROM no-such-index-for-view", true);
        assertThat(response.getWarnings(), hasSize(1));
        assertThat(response.getWarnings().getFirst(), containsString("view [view-validation-unknown-index] query is not valid"));
        assertThat(response.getWarnings().getFirst(), containsString("Unknown index [no-such-index-for-view]"));

        // The view is saved regardless.
        ensureViewExists("view-validation-unknown-index");
    }

    public void testUnknownColumnEmitsWarning() throws IOException {
        createIndex("view-validation-index", null, "\"properties\": {\"k\": {\"type\": \"keyword\"}}");
        Response response = putView("view-validation-unknown-column", "FROM view-validation-index | KEEP no_such_column", true);
        assertThat(response.getWarnings(), hasSize(1));
        assertThat(response.getWarnings().getFirst(), containsString("Unknown column [no_such_column]"));

        // The view is saved regardless.
        ensureViewExists("view-validation-unknown-column");
    }

    public void testUnknownRemoteEmitsWarning() throws IOException {
        Response response = putView("view-validation-unknown-remote", "FROM remote:view-validation-index", true);
        assertThat(response.getWarnings(), hasSize(1));
        assertThat(response.getWarnings().getFirst(), containsString("no such remote cluster: [remote]"));

        // The view is saved regardless.
        ensureViewExists("view-validation-unknown-remote");
    }

    private Response putView(String name, String query, boolean validate) throws IOException {
        Request request = new Request("PUT", "/_query/view/" + name);
        request.addParameter("validate", Boolean.toString(validate));
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return client().performRequest(request);
    }

    private void ensureViewExists(String name) throws IOException {
        assertThat(client().performRequest(new Request("GET", "/_query/view/" + name)).getStatusLine().getStatusCode(), equalTo(200));
    }
}
