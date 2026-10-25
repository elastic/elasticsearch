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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that run ESQL queries over views that could use a ton of memory when they are expanded. We want to make sure they don't consume
 * the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE)
public class HeapAttackViewIT extends HeapAttackTestCase {

    /**
     * Views that reference the view below them several times each expand to {@code refsPerView ^ levels} leaves
     * The query must be rejected due to excess of planning nodes
     */
    public void testExponentialViewsExpansionRejected() throws IOException {
        initSingleDocIndex();
        String view = "heap_attack_view_";
        int levels = 7;
        int refsPerView = 15;

        try {
            createViewsThatReferenceViewsManyTimes(view, "FROM single | WHERE a > 0", levels, refsPerView);
            StringBuilder query = startQuery();
            String viewName = view + levels;
            query.append("FROM ").append(viewName).append(" | STATS c = COUNT(*) \"}");
            assertRejectedWithError(
                query.toString(),
                "ES|QL query exceeded the maximum of [500] plan nodes allowed after expanding views, subqueries and FORKs."
            );
        } finally {
            deleteViews(view, levels);
        }
    }

    /**
     * Same shape as {@link #testExponentialViewsExpansionRejected} but every view body is a bare source command. Such bodies add no
     * processing commands, so the parser-side command count never trips; only the plan node count after expansion rejects them.
     */
    public void testExponentialViewsExpansionWithOnlySourceCommandsRejected() throws IOException {
        initSingleDocIndex();
        String view = "heap_attack_view_";
        int levels = 7;
        int refsPerView = 15;

        try {
            createViewsThatReferenceViewsManyTimes(view, "FROM single", levels, refsPerView);
            StringBuilder query = startQuery();
            String viewName = view + levels;
            query.append("FROM ").append(viewName).append(" | STATS c = COUNT(*) \"}");
            assertRejectedWithError(
                query.toString(),
                "ES|QL query exceeded the maximum of [500] plan nodes allowed after expanding views, subqueries and FORKs."
            );
        } finally {
            deleteViews(view, levels);
        }
    }

    /**
     * Wide expressions in a tree of distinct views that stays below {@code EsqlFlags.ESQL_MAX_COMMAND_COUNT} plan nodes, so it is not
     * rejected by the plan size check. It is rejected later, by the branch limit, before the expanded plan can use much heap.
     * Expression nodes do not count towards the plan size.
     */
    public void testWideExpressionsInViewsRejectedByBranchLimit() throws IOException {
        initSingleDocIndex();
        String prefix = "heap_attack_wide_expr_tree_view_";
        // 1 + 4 + 16 + 64 = 85 view bodies with 64 leaf branches
        int levels = 4;
        int refsPerView = 4;
        // the widest expression that fits in a view definition (views are limited to 10000 characters)
        int argsPerExpression = 2300;
        List<String> createdViews = new ArrayList<>();
        try {
            String root = createWideExpressionViewTree(prefix, levels, refsPerView, wideExpression(argsPerExpression), createdViews);
            StringBuilder query = startQuery();
            query.append("FROM ").append(root).append(" | STATS c = COUNT(*) \"}");
            assertRejectedWithError(query.toString(), "exceeding the limit of 20 set by the [esql.query.max_branch_count] cluster setting");
        } finally {
            deleteViews(createdViews);
        }
    }

    private static String wideExpression(int args) {
        StringBuilder arguments = new StringBuilder("a+a");
        for (int i = 1; i < args; i++) {
            arguments.append(",a+a");
        }
        return "COALESCE(" + arguments + ")";
    }

    /**
     * Creates a tree of distinct views, each with a wide expression, and returns the name of the root view. Views are distinct because
     * referencing the same view twice at one level is not what is under test here.
     */
    private String createWideExpressionViewTree(String prefix, int level, int refsPerView, String expression, List<String> createdViews)
        throws IOException {
        String query;
        if (level == 1) {
            query = "FROM single | EVAL y = " + expression;
        } else {
            List<String> children = new ArrayList<>();
            for (int i = 0; i < refsPerView; i++) {
                children.add("(FROM " + createWideExpressionViewTree(prefix, level - 1, refsPerView, expression, createdViews) + ")");
            }
            query = "FROM " + String.join(", ", children) + " | EVAL y = " + expression;
        }
        // Named after the children are created, so the index is unique within the tree.
        String name = prefix + createdViews.size();
        createView(name, query);
        createdViews.add(name);
        return name;
    }

    private void createViewsThatReferenceViewsManyTimes(String prefix, String viewDefinition, int levels, int referencesPerView)
        throws IOException {
        createView(prefix + 1, viewDefinition);
        for (int level = 2; level <= levels; level++) {
            String reference = "(FROM " + prefix + (level - 1) + ")";
            StringBuilder query = new StringBuilder("FROM ").append(reference);
            for (int i = 1; i < referencesPerView; i++) {
                query.append(", ").append(reference);
            }
            createView(prefix + level, query.toString());
        }
    }

    private void createView(String name, String query) throws IOException {
        Request request = new Request("PUT", "/_query/view/" + name);
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        client().performRequest(request);
    }

    private void deleteViews(List<String> names) {
        for (String name : names.reversed()) {
            try {
                client().performRequest(new Request("DELETE", "/_query/view/" + name));
            } catch (IOException e) {
                logger.warn("failed to delete view [{}]", name, e);
            }
        }
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

    private void assertRejectedWithError(String query, String expectedReason) throws IOException {
        try {
            Map<String, Object> response = responseAsMap(query(query, "columns,values"));
            fail("expected the query to be rejected but got " + response);
        } catch (ResponseException e) {
            assertMap(
                responseAsMap(e.getResponse()),
                matchesMap().extraOk()
                    .entry("status", 400)
                    .entry(
                        "error",
                        matchesMap().extraOk().entry("type", "verification_exception").entry("reason", containsString(expectedReason))
                    )
            );
        }
    }
}
