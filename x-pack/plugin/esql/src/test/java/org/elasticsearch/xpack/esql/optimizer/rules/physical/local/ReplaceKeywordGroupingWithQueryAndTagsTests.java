/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ReplaceKeywordGroupingWithQueryAndTagsTests extends AbstractLocalPhysicalPlanOptimizerTests {
    public ReplaceKeywordGroupingWithQueryAndTagsTests(String name, Configuration config) {
        super(name, config);
    }

    public void testCountByKeyword() {
        List<EsQueryExec.QueryBuilderAndTags> queries = pushedQueries(plan("""
            from test
            | stats count(*) by keyword
            """, statsWithTerms("keyword", "a", "b")));

        assertThat(queries, hasSize(3));
        assertBucket(queries.get(0), termQuery("keyword", new BytesRef("a")), new BytesRef("a"));
        assertBucket(queries.get(1), termQuery("keyword", new BytesRef("b")), new BytesRef("b"));
        assertBucket(queries.get(2), boolQuery().mustNot(existsQuery("keyword").boost(0)), null);
    }

    public void testCountByKeywordWithIsNotNull() {
        List<EsQueryExec.QueryBuilderAndTags> queries = pushedQueries(plan("""
            from test
            | where keyword is not null
            | stats count(*) by keyword
            """, statsWithTerms("keyword", "a", "b")));

        assertThat(queries, hasSize(2));
        assertBucket(queries.get(0), termQuery("keyword", new BytesRef("a")), new BytesRef("a"));
        assertBucket(queries.get(1), termQuery("keyword", new BytesRef("b")), new BytesRef("b"));
    }

    public void testCountByKeywordWithOtherFilterIsNotRewritten() {
        assertNotRewritten(plan("""
            from test
            | where integer > 1
            | stats count(*) by keyword
            """, statsWithTerms("keyword", "a", "b")));
    }

    public void testCountAndOtherAggregateByKeywordIsNotRewritten() {
        assertNotRewritten(plan("""
            from test
            | stats count(*), max(integer) by keyword
            """, statsWithTerms("keyword", "a", "b")));
    }

    public void testCountByKeywordWithoutTermsIsNotRewritten() {
        assertNotRewritten(plan("""
            from test
            | stats count(*) by keyword
            """, EsqlTestUtils.TEST_SEARCH_STATS));
    }

    public void testCountByKeywordAboveThresholdIsNotRewritten() {
        String query = """
            from test
            | stats count(*) by keyword
            """;
        SearchStats stats = statsWithTerms("keyword", "a", "b", "c");
        int threshold = between(0, 2);
        TestPlannerOptimizer optimizer = new TestPlannerOptimizer(
            configuration(
                new QueryPragmas(
                    Settings.builder().put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey().toLowerCase(Locale.ROOT), threshold).build()
                ),
                query
            ),
            makeAnalyzer("mapping-all-types.json")
        );
        assertNotRewritten(optimizer.plan(query, stats, new EsqlFlags(-1)));
    }

    private PhysicalPlan plan(String query, SearchStats stats) {
        return plannerOptimizer.plan(query, stats, makeAnalyzer("mapping-all-types.json"));
    }

    private static SearchStats statsWithTerms(String field, String... values) {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public List<BytesRef> keywordTerms(FieldName name, int maxTerms) {
                return name.string().equals(field) && values.length <= maxTerms ? Arrays.stream(values).map(BytesRef::new).toList() : null;
            }
        };
    }

    private static ExchangeExec exchange(PhysicalPlan plan) {
        LimitExec limit = as(plan, LimitExec.class);
        AggregateExec agg = as(limit.child(), AggregateExec.class);
        return as(agg.child(), ExchangeExec.class);
    }

    private static List<EsQueryExec.QueryBuilderAndTags> pushedQueries(PhysicalPlan plan) {
        FilterExec filter = as(exchange(plan).child(), FilterExec.class);
        EsStatsQueryExec stats = as(filter.child(), EsStatsQueryExec.class);
        assertThat(
            stats.output().stream().map(Attribute::dataType).toList(),
            equalTo(List.of(DataType.KEYWORD, DataType.LONG, DataType.BOOLEAN))
        );
        return as(stats.stat(), EsStatsQueryExec.ByStat.class).queryBuilderAndTags();
    }

    private static void assertBucket(EsQueryExec.QueryBuilderAndTags bucket, QueryBuilder query, BytesRef tag) {
        assertThat(bucket.query(), equalTo(query));
        assertThat(bucket.tags(), hasSize(1));
        assertThat(bucket.tags().getFirst(), tag == null ? nullValue() : equalTo(tag));
    }

    private static void assertNotRewritten(PhysicalPlan plan) {
        assertThat(plan.collectFirstChildren(EsStatsQueryExec.class::isInstance), empty());
        EsQueryExec queryExec = (EsQueryExec) plan.collectFirstChildren(EsQueryExec.class::isInstance).getFirst();
        assertThat(queryExec.queryBuilderAndTags(), hasSize(1));
        assertThat(queryExec.queryBuilderAndTags().getFirst().tags(), empty());
    }
}
