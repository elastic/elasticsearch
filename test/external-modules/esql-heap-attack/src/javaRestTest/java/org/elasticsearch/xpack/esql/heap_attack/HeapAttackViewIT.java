/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests that run ESQL queries over views that use a ton of memory when they are expanded. We want to make sure they don't consume the
 * entire heap and crash Elasticsearch.
 * <p>
 * The size of a query that uses views is not bounded by its text. A view body may reference the same view several times, and each
 * reference is expanded in full. The limits on the number of commands on a path, on the number of views, and on the length of a view
 * bound the height of the expanded plan and the text of each view, but not how many times a view is expanded: views that reference
 * the views below them {@code N} times each expand to {@code N^depth} leaves. The branch limits that would reject such a query are
 * checked only after the whole plan has been built.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackViewIT extends HeapAttackTestCase {

    private static final String VIEW_PREFIX = "heap_attack_view_";

    /**
     * Levels of views that reference the views below them. Must not exceed {@code esql.views.max_view_depth}, which defaults to 10, as
     * that limit would reject the query before it grows.
     */
    private static final int VIEW_LEVELS = 7;

    /**
     * How many times each view references the view below it. The first view is copied {@code 8^6} times, which makes a plan of about
     * 1.2 million nodes. That is more than the heap of the heap-attack cluster can hold, while each view is a hundred characters long.
     */
    private static final int REFERENCES_PER_VIEW = 8;

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    /**
     * Views that reference the view below them several times each expand to {@code REFERENCES_PER_VIEW ^ VIEW_LEVELS} leaves, without
     * any command on any path going near the limit. The query must be rejected, as a client error or by the circuit breaker, before the
     * expansion uses the heap of the node it runs on.
     */
    public void testViewsThatReferenceViewsManyTimes() throws IOException {
        initSingleDocIndex();
        try {
            createViewsThatReferenceViewsManyTimes();
            StringBuilder query = startQuery();
            query.append("FROM ").append(VIEW_PREFIX).append(VIEW_LEVELS).append(" | STATS c = COUNT(*) \"}");
            Map<String, Object> response = responseAsMap(query(query.toString(), "columns,values"));
            fail("expected the query to be rejected but got " + response);
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            assertThat(
                "the query must fail with a client error or a circuit breaker, got " + e,
                status,
                allOf(greaterThanOrEqualTo(400), lessThan(500))
            );
        } finally {
            deleteViews();
        }
    }

    /**
     * Creates {@code heap_attack_view_1} to {@code heap_attack_view_<VIEW_LEVELS>}. The first reads the index, each of the others
     * references the one below it {@link #REFERENCES_PER_VIEW} times. Each view ends in a command so that it is not a bare reference to
     * the view below it, which view resolution would compact away.
     */
    private void createViewsThatReferenceViewsManyTimes() throws IOException {
        createView(VIEW_PREFIX + 1, "FROM single | WHERE a > 0");
        for (int level = 2; level <= VIEW_LEVELS; level++) {
            String reference = "(FROM " + VIEW_PREFIX + (level - 1) + ")";
            StringBuilder query = new StringBuilder("FROM ").append(reference);
            for (int i = 1; i < REFERENCES_PER_VIEW; i++) {
                query.append(", ").append(reference);
            }
            query.append(" | WHERE a > 0");
            createView(VIEW_PREFIX + level, query.toString());
        }
    }

    private void createView(String name, String query) throws IOException {
        Request request = new Request("PUT", "/_query/view/" + name);
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        client().performRequest(request);
    }

    private void deleteViews() {
        for (int level = VIEW_LEVELS; level >= 1; level--) {
            try {
                client().performRequest(new Request("DELETE", "/_query/view/" + VIEW_PREFIX + level));
            } catch (IOException e) {
                logger.warn("failed to delete view [{}{}]", VIEW_PREFIX, level, e);
            }
        }
    }
}
