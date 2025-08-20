/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;

/**
 * Tests for the {@link IgnoreNullMetrics} transformation rule.  Like most rule tests, this runs the entire analysis chain.
 */
public class IgnoreNullMetricsTests extends ESTestCase {

    private Analyzer analyzer;

    private LogicalPlan analyze(String query) {
        EsqlParser parser = new EsqlParser();
        EnrichResolution enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");

        Map<String, EsField> mapping = Map.of(
            "dimension_1",
            new EsField("dimension_1", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "dimension_2",
            new EsField("dimension_2", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "metric_1",
            new EsField("metric_1", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "metric_2",
            new EsField("metric_2", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.TIME_SERIES));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        return analyzer.analyze(parser.createStatement(query, EsqlTestUtils.TEST_CFG));
    }

    public void testSimple() {
        LogicalPlan actual = analyze("""
            TS test
            | STATS max(max_over_time(metric_1))
            | LIMIT 10
            """);
        Limit limit_10000 = as(actual, Limit.class);
        Limit limit_10 = as(limit_10000.child(), Limit.class);
        Aggregate agg = as(limit_10.child(), Aggregate.class);
        Filter filter = as(agg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testDimensionsAreNotFiltered() {

        LogicalPlan actual = analyze("""
            TS test
            | STATS max(max_over_time(metric_1)) BY dimension_1
            | LIMIT 10
            """);
        Limit limit_10000 = as(actual, Limit.class);
        Limit limit_10 = as(limit_10000.child(), Limit.class);
        Aggregate agg = as(limit_10.child(), Aggregate.class);
        Filter filter = as(agg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testFiltersAreJoinedWithOr() {

        LogicalPlan actual = analyze("""
            TS test
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit_10000 = as(actual, Limit.class);
        Limit limit_10 = as(limit_10000.child(), Limit.class);
        Aggregate agg = as(limit_10.child(), Aggregate.class);
        Filter filter = as(agg.child(), Filter.class);
        Or or = as(filter.expressions().getFirst(), Or.class);

        // For reasons beyond my comprehension, the ordering of the conditionals inside the OR is nondeterministic.
        IsNotNull condition;
        FieldAttribute attribute;

        condition = as(or.left(), IsNotNull.class);
        attribute = as(condition.field(), FieldAttribute.class);
        if (attribute.fieldName().string().equals("metric_1")) {
            condition = as(or.right(), IsNotNull.class);
            attribute = as(condition.field(), FieldAttribute.class);
            assertEquals("metric_2", attribute.fieldName().string());
        } else if (attribute.fieldName().string().equals("metric_2")) {
            condition = as(or.right(), IsNotNull.class);
            attribute = as(condition.field(), FieldAttribute.class);
            assertEquals("metric_1", attribute.fieldName().string());
        } else {
            // something weird happened
            assert false;
        }

    }

    public void testSkipCoalescedMetrics() {
        // Note: this test is passing because the reference attribute metric_2 in the stats block does not inherit the
        // metric property from the original field.
        LogicalPlan actual = analyze("""
            TS test
            | EVAL metric_2 = coalesce(metric_2, 0)
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit_10000 = as(actual, Limit.class);
        Limit limit_10 = as(limit_10000.child(), Limit.class);
        Aggregate agg = as(limit_10.child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }
}
