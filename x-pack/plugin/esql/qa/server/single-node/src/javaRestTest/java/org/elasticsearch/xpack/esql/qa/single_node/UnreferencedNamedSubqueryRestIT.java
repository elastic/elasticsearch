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
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * A {@code LET} named subquery that nothing references is not part of the query that runs. It is analyzed all the same, so that
 * a mistake in it is reported, and it gets a warning.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class UnreferencedNamedSubqueryRestIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    private static final String INDEX = "unreferenced-let-index";

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws IOException {
        if (indexExists(INDEX) == false) {
            createIndex(INDEX, null, "\"properties\": {\"k\": {\"type\": \"keyword\"}, \"n\": {\"type\": \"integer\"}}");
        }
    }

    public void testReferencedNamedSubqueryHasNoWarning() throws IOException {
        assertThat(letWarnings(query("LET used = (FROM " + INDEX + " | KEEP k); FROM used")), empty());
    }

    public void testUnreferencedNamedSubqueryEmitsWarning() throws IOException {
        Response response = query("LET unused = (FROM " + INDEX + " | KEEP k); ROW a = 1");
        assertThat(letWarnings(response), contains("Line 1:5: LET subquery [unused] is declared but never referenced"));
        assertThat(ObjectPath.createFromResponse(response).evaluate("values"), equalTo(List.of(List.of(1))));
    }

    public void testEachUnreferencedNamedSubqueryEmitsWarning() throws IOException {
        Response response = query("LET a = (ROW x = 1); LET b = (ROW y = 2); LET c = (ROW z = 3); FROM b");
        assertThat(
            letWarnings(response),
            contains(
                "Line 1:5: LET subquery [a] is declared but never referenced",
                "Line 1:47: LET subquery [c] is declared but never referenced"
            )
        );
        assertThat(ObjectPath.createFromResponse(response).evaluate("values"), equalTo(List.of(List.of(2))));
    }

    /**
     * A named subquery read only by an unreferenced one is analyzed as part of that one, and only the outer one is warned about.
     */
    public void testChainedUnreferencedNamedSubqueries() throws IOException {
        assertThat(
            letWarnings(query("LET a = (FROM " + INDEX + " | KEEP k); LET b = (FROM a | LIMIT 1); ROW r = 1")),
            contains(containsString("LET subquery [b] is declared but never referenced"))
        );
        // b cannot see n, because a drops it
        assertQueryFails("LET a = (FROM " + INDEX + " | KEEP k); LET b = (FROM a | WHERE n > 1); ROW r = 1", "Unknown column [n]");
    }

    public void testUnknownIndexInUnreferencedNamedSubqueryFailsTheQuery() {
        assertQueryFails("LET unused = (FROM no-such-index-for-let); ROW a = 1", "Unknown index [no-such-index-for-let]");
    }

    public void testUnknownFunctionInUnreferencedNamedSubqueryFailsTheQuery() {
        assertQueryFails("LET unused = (FROM " + INDEX + " | EVAL x = no_such_fn(k)); ROW a = 1", "Unknown function [no_such_fn]");
    }

    public void testUnknownColumnInUnreferencedNamedSubqueryFailsTheQuery() {
        assertQueryFails("LET unused = (FROM " + INDEX + " | KEEP no_such_column); ROW a = 1", "Unknown column [no_such_column]");
    }

    public void testTypeErrorInUnreferencedNamedSubqueryFailsTheQuery() {
        assertQueryFails(
            "LET unused = (FROM " + INDEX + " | EVAL x = k + 1); ROW a = 1",
            "first argument of [k + 1] must be [date_nanos, datetime, numeric or dense_vector], found value [k] type [keyword]"
        );
    }

    public void testUnreferencedNamedSubqueryInViewEmitsWarningWhenTheViewIsQueried() throws IOException {
        putView("unreferenced-let-view", "LET unused = (FROM " + INDEX + " | KEEP k); FROM " + INDEX + " | KEEP n");
        assertThat(
            letWarnings(query("FROM unreferenced-let-view")),
            contains("Line 1:5 (in view [unreferenced-let-view]): LET subquery [unused] is declared but never referenced")
        );
    }

    public void testInvalidUnreferencedNamedSubqueryInViewFailsQueriesOfTheView() throws IOException {
        putView("unreferenced-let-invalid-view", "LET unused = (FROM no-such-index-for-let); FROM " + INDEX + " | KEEP n");
        assertQueryFails("FROM unreferenced-let-invalid-view", "Unknown index [no-such-index-for-let]");
    }

    /**
     * The unreferenced named subquery of a view reads the view itself. Analyzing it resolves the view once more, and must stop there.
     */
    public void testUnreferencedNamedSubqueryReadingItsOwnView() throws IOException {
        putView("unreferenced-let-self-view", "LET unused = (FROM unreferenced-let-self-view); FROM " + INDEX + " | KEEP n");
        assertThat(
            letWarnings(query("FROM unreferenced-let-self-view")),
            contains("Line 1:5 (in view [unreferenced-let-self-view]): LET subquery [unused] is declared but never referenced")
        );
    }

    /**
     * The warnings of the response, without the one about the default limit that most of these queries get.
     */
    private static List<String> letWarnings(Response response) {
        return response.getWarnings().stream().filter(w -> w.startsWith("No limit defined") == false).toList();
    }

    private void assertQueryFails(String query, String message) {
        ResponseException e = expectThrows(ResponseException.class, containsString(message), () -> query(query));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    private Response query(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return client().performRequest(request);
    }

    private void putView(String name, String query) throws IOException {
        Request request = new Request("PUT", "/_query/view/" + name);
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        client().performRequest(request);
    }
}
