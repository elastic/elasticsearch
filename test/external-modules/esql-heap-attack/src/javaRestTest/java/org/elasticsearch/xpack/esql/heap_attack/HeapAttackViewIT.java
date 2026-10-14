/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that run ESQL queries over views that use a ton of memory when they are expanded. We want to make sure they don't consume the
 * entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 10 * TimeUnits.MINUTE)
public class HeapAttackViewIT extends HeapAttackTestCase {

    /**
     * Views that reference the view below them several times each expand to {@code refsPerView ^ levels} leaves, without
     * any command on any path going near the limit. The query must be rejected due to excess of planning nodes
     */
    public void testExpandedViewOverTheLimitPlanSize() throws IOException {
        initSingleDocIndex();
        String view = "heap_attack_view_";
        int levels = 7;
        int refsPerView = 8;

        try {
            createViewsThatReferenceViewsManyTimes(view, levels, refsPerView);
            assertRejectedWithMaxPlanSizeError(view + levels);
        } finally {
            deleteViews(view, levels);
        }
    }

    private void assertRejectedWithMaxPlanSizeError(String viewName) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM ").append(viewName).append(" | STATS c = COUNT(*) \"}");
        try {
            Map<String, Object> response = responseAsMap(query(query.toString(), "columns,values"));
            fail("expected the query to be rejected but got " + response);
        } catch (ResponseException e) {
            assertMap(
                responseAsMap(e.getResponse()),
                matchesMap().extraOk()
                    .entry("status", 400)
                    .entry(
                        "error",
                        matchesMap().extraOk()
                            .entry("type", "verification_exception")
                            .entry("reason", containsString("ES|QL query exceeded the maximum plan size allowed after expanding views"))
                    )
            );
        }
    }

    /**
     * Creates {@code <prefix>1} to {@code <prefix><levels>}. The first reads the index, each of the others references the one below it
     * {@code referencesPerView} times. Each view ends in a command so that it is not a bare reference to the view below it, which view
     * resolution would compact away.
     */
    private void createViewsThatReferenceViewsManyTimes(String prefix, int levels, int referencesPerView) throws IOException {
        createView(prefix + 1, "FROM single | WHERE a > 0");
        for (int level = 2; level <= levels; level++) {
            String reference = "(FROM " + prefix + (level - 1) + ")";
            StringBuilder query = new StringBuilder("FROM ").append(reference);
            for (int i = 1; i < referencesPerView; i++) {
                query.append(", ").append(reference);
            }
            query.append(" | WHERE a > 0");
            createView(prefix + level, query.toString());
        }
    }

    private void createView(String name, String query) throws IOException {
        Request request = new Request("PUT", "/_query/view/" + name);
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        client().performRequest(request);
    }

    private void deleteViews(String prefix, int levels) {
        for (int level = levels; level >= 1; level--) {
            try {
                client().performRequest(new Request("DELETE", "/_query/view/" + prefix + level));
            } catch (IOException e) {
                logger.warn("failed to delete view [{}{}]", prefix, level, e);
            }
        }
    }
}
