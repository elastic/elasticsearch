/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for {@link PushCountByKeywordToSource}, with the distinct values of the fields that {@link SearchStats} lists
 * given by the test rather than read from an index.
 */
public class PushCountByKeywordToSourceTests extends AbstractLocalPhysicalPlanOptimizerTests {

    public PushCountByKeywordToSourceTests(String name, Configuration config) {
        super(name, config);
    }

    public void testCountByKeyword() {
        for (String query : List.of(
            "from test | stats count(*) by keyword",
            "from test | stats c = count(*) by k = keyword",
            "from test | stats c = count(*) by keyword | keep c",
            "from test | stats c = count(*), m = max(integer) by keyword | keep c, keyword"
        )) {
            List<EsQueryExec.QueryBuilderAndTags> queries = pushedQueries(plan(query, statsWithValues("a", "b")));
            List<Object> nullTags = new ArrayList<>(1);
            nullTags.add(null);
            List<EsQueryExec.QueryBuilderAndTags> expected = List.of(
                new EsQueryExec.QueryBuilderAndTags(termQuery("keyword", "a").boost(0), List.of(new BytesRef("a"))),
                new EsQueryExec.QueryBuilderAndTags(termQuery("keyword", "b").boost(0), List.of(new BytesRef("b"))),
                new EsQueryExec.QueryBuilderAndTags(boolQuery().mustNot(existsQuery("keyword").boost(0)).boost(0), nullTags)
            );
            assertThat(query, queries, hasSize(expected.size()));
            for (int i = 0; i < expected.size(); i++) {
                // The queries are compared as they are rendered, like in the other tests of queries with tags
                assertThat(query, queries.get(i).query().toString(), equalTo(expected.get(i).query().toString()));
                assertThat(query, queries.get(i).tags(), equalTo(expected.get(i).tags()));
            }
        }
    }

    public void testNotPushed() {
        for (String query : List.of(
            // Not a keyword
            "from test | stats count(*) by integer",
            "from test | stats count(*) by text",
            "from test | stats count(*) by ip",
            // Not a field
            "from test | stats count(*) by k = to_lower(keyword)",
            "from test | stats count(*) by k = concat(keyword, \"x\")",
            // Not only the count of the documents
            "from test | stats count(*), max(integer) by keyword",
            "from test | stats count(integer) by keyword",
            "from test | stats count(keyword) by keyword",
            "from test | stats count_distinct(integer) by keyword",
            "from test | stats count(*) where integer > 1 by keyword",
            "from test | stats count(*) by keyword, integer",
            // Not all the documents
            "from test | where integer > 1 | stats count(*) by keyword",
            "from test | where keyword == \"a\" | stats count(*) by keyword",
            "from test | limit 10 | stats count(*) by keyword",
            "from test | sort integer | limit 10 | stats count(*) by keyword"
        )) {
            assertThat(query, statsQueries(plan(query, statsWithValues("a", "b"))), empty());
        }
    }

    public void testValuesNotListed() {
        String query = "from test | stats count(*) by keyword";
        assertThat(statsQueries(plan(query, EsqlTestUtils.TEST_SEARCH_STATS)), empty());
        assertThat(statsQueries(plan(query, statsWithValues())), empty());
    }

    /**
     * There is a query for every value and one for the documents with no value, up to the threshold for queries with tags.
     */
    public void testThreshold() {
        String query = "from test | stats count(*) by keyword";
        TestPlannerOptimizer optimizer = new TestPlannerOptimizer(config, makeAnalyzer("mapping-all-types.json"));
        SearchStats stats = statsWithValues("a", "b", "c");
        assertThat(pushedQueries(optimizer.plan(query, stats, new EsqlFlags(4))), hasSize(4));
        assertThat(statsQueries(optimizer.plan(query, stats, new EsqlFlags(3))), empty());
        assertThat(statsQueries(optimizer.plan(query, stats, new EsqlFlags(1))), empty());
        assertThat(statsQueries(optimizer.plan(query, stats, new EsqlFlags(0))), empty());
    }

    private PhysicalPlan plan(String query, SearchStats stats) {
        return plannerOptimizer.plan(query, stats, makeAnalyzer("mapping-all-types.json"));
    }

    /**
     * The queries of the count that is pushed down, below the filter that removes the values with no documents.
     */
    private static List<EsQueryExec.QueryBuilderAndTags> pushedQueries(PhysicalPlan plan) {
        List<PhysicalPlan> statsQueries = statsQueries(plan);
        assertThat(plan.toString(), statsQueries, hasSize(1));
        assertThat(plan.collectFirstChildren(p -> p instanceof FilterExec filter && filter.child() == statsQueries.getFirst()), hasSize(1));
        EsStatsQueryExec statsQuery = as(statsQueries.getFirst(), EsStatsQueryExec.class);
        return as(statsQuery.stat(), EsStatsQueryExec.ByStat.class).queryBuilderAndTags();
    }

    private static List<PhysicalPlan> statsQueries(PhysicalPlan plan) {
        return plan.collectFirstChildren(EsStatsQueryExec.class::isInstance);
    }

    /**
     * Stats that list the given values as the distinct values of any field, as long as they are within the limit.
     */
    private static SearchStats statsWithValues(String... values) {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public List<BytesRef> distinctValues(FieldAttribute.FieldName field, int limit) {
                if (values.length > limit) {
                    return null;
                }
                List<BytesRef> result = new ArrayList<>(values.length);
                for (String value : values) {
                    result.add(new BytesRef(value));
                }
                return result;
            }
        };
    }
}
