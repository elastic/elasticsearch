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
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Tests that run ESQL queries with subqueries that use a ton of memory. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    // the upper limit is defined in {@code Fork.MAX_BRANCHES}
    private static final int MAX_SUBQUERIES = 8;

    public void testManyKeywordFieldsInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        heapAttackIT.initManyBigFieldsIndex(500, "keyword");
        Map<?, ?> response = buildSubqueries(2, "manybigfields");
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testManyKeywordFieldsInSubqueryIntermediateResultsWithAgg() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword");
        Map<?, ?> response = buildSubqueriesWithAgg(1, "manybigfields");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(response, matchesMap().entry("columns", columns).entry("values", matchesList().item(matchesList().item(1024 * docs))));
    }

    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        // TODO 25 docs + 2 subqueries without "limit 40" in the main query causes OOM, should we CBE instead?
        heapAttackIT.initGiantTextField(100);
        Map<?, ?> response = buildSubqueries(2, "bigtext");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "f").entry("type", "text"));
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithAgg() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 100;
        heapAttackIT.initGiantTextField(docs);
        Map<?, ?> response = buildSubqueriesWithAgg(2, "bigtext");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns)
                .entry(
                    "values",
                    matchesList().item(matchesList().item(1024 * 1024 * 5 * docs)).item(matchesList().item(1024 * 1024 * 5 * docs))
                )
        );
    }

    public void testManySubqueriesWithManyKeywordFields() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        heapAttackIT.initManyBigFieldsIndex(500, "keyword");
        Map<?, ?> response = buildSubqueries(MAX_SUBQUERIES, "manybigfields");
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testManySubqueriesWithManyKeywordFieldsWithAgg() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword");
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns)
                .entry(
                    "values",
                    matchesList().item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                        .item(matchesList().item(1024 * docs))
                )
        );
    }

    public void testManySubqueriesWithGiantTextField() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        // TODO 8 docs * 8 subqueries with "limit 40" in the main query causes OOM, should we CBE instead?
        heapAttackIT.initGiantTextField(5);
        Map<?, ?> response = buildSubqueries(MAX_SUBQUERIES, "bigtext");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "f").entry("type", "text"));
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testManySubqueriesWithGiantTextFieldWithAgg() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 100; // 500 docs didn't OOM or CB
        heapAttackIT.initGiantTextField(docs);
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "bigtext");
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns)
                .entry(
                    "values",
                    matchesList().item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                        .item(matchesList().item(1024 * 1024 * 5 * docs))
                )
        );
    }

    private Map<String, Object> buildSubqueries(int subqueries, String indexName) throws IOException {
        StringBuilder query = startQuery();
        // TODO remove the limit after fixing the OOM(limit 50 OOM locally) on bigtext, should we CBE instead?
        String limit = indexName.equalsIgnoreCase("bigtext") ? " | LIMIT 10 " : "";
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(limit).append(" \"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildSubqueriesWithAgg(int subqueries, String indexName) throws IOException {
        StringBuilder query = startQuery();
        String agg = indexName.equalsIgnoreCase("bigtext") ? "SUM(LENGTH(f))" : "SUM(LENGTH(f999))";
        String subquery = "(FROM " + indexName + " | STATS sum = " + agg + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" \"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }
}
