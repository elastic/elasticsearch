/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Lucene refuses nested queries where {@code search.allow_expensive_queries} is {@code false}, so a {@code NESTED_ANY} must keep
 * reading the child documents there instead of being pushed to Lucene or evaluated by it.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class NestedAnyExpensiveQueriesIT extends AbstractEsqlIntegTestCase {
    @Before
    public void createOrders() {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        assertAcked(prepareCreate("orders").setMapping("""
            {"properties": {"id": {"type": "keyword"}, "items": {"type": "nested", "properties": {"price": {"type": "long"}}}}}
            """));
        prepareIndex("orders").setId("a").setSource("""
            {"id": "a", "items": [{"price": 20}, {"price": 5}]}""", XContentType.JSON).get();
        prepareIndex("orders").setId("b").setSource("""
            {"id": "b", "items": [{"price": 1}]}""", XContentType.JSON).get();
        prepareIndex("orders").setId("c").setSource("""
            {"id": "c"}""", XContentType.JSON).get();
        refresh("orders");
    }

    @After
    public void resetExpensiveQueries() {
        updateClusterSettings(Settings.builder().putNull(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()));
    }

    public void testNestedPredicatesWorkWhereExpensiveQueriesAreAllowed() {
        assertQueries();
    }

    public void testNestedPredicatesWorkWhereExpensiveQueriesAreRefused() {
        updateClusterSettings(Settings.builder().put(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey(), false));
        assertQueries();
    }

    private void assertQueries() {
        assertRows("FROM orders | WHERE NESTED_ANY(items.price > 10) | KEEP id | SORT id", List.of(List.of("a")));
        assertRows("FROM orders | WHERE NESTED_ANY(items, true) | KEEP id | SORT id", List.of(List.of("a"), List.of("b")));
        assertRows(
            "FROM orders | EVAL h = NESTED_ANY(items.price > 10) | KEEP id, h | SORT id",
            List.of(List.of("a", true), List.of("b", false), List.of("c", false))
        );
    }

    private void assertRows(String query, List<List<Object>> expected) {
        try (EsqlQueryResponse response = run(query)) {
            assertThat(getValuesList(response), equalTo(expected));
        }
    }
}
