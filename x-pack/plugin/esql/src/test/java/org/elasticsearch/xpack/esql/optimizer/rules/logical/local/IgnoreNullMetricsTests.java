/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.BeforeClass;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the {@link IgnoreNullMetrics} transformation rule.  Like most rule tests, this runs the entire analysis chain.
 */
public class IgnoreNullMetricsTests extends ESTestCase {

    private static Analyzer analyzer;
    private static EsqlParser parser;
    private static LogicalPlanOptimizer logicalOptimizer;

    @BeforeClass
    private static void init() {
        parser = new EsqlParser();
        EnrichResolution enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");
        LogicalOptimizerContext logicalOptimizerCtx = unboundLogicalOptimizerContext();
        logicalOptimizer = new LogicalPlanOptimizer(logicalOptimizerCtx);

        Map<String, EsField> mapping = Map.of(
            "dimension_1",
            new EsField("dimension_1", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "dimension_2",
            new EsField("dimension_2", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "metric_1",
            new EsField("metric_1", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "metric_2",
            new EsField("metric_2", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "_tsid",
            new EsField("_tsid", DataType.TSID_DATA_TYPE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.TIME_SERIES));
        analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    private LogicalPlan plan(String query, Analyzer analyzer) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        return logicalOptimizer.optimize(analyzed);
    }

    protected LogicalPlan plan(String query) {
        return plan(query, analyzer);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, SearchStats searchStats) {
        LocalLogicalOptimizerContext localContext = new LocalLogicalOptimizerContext(
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            searchStats
        );
        return new LocalLogicalPlanOptimizer(localContext).localOptimize(plan);
    }

    private LogicalPlan localPlan(String query) {
        return localPlan(plan(query), TEST_SEARCH_STATS);
    }

    public void testSimple() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval bucketEval = as(tsAgg.child(), Eval.class);
        Filter filter = as(bucketEval.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testRuleDoesNotApplyInNonTSMode() {
        // NOTE: it is necessary to have the `BY dimension 1` grouping here, otherwise the InfernonNullAggConstraint rule
        // will add the same filter IgnoreNullMetrics would have.
        LogicalPlan actual = localPlan("""
            FROM test
            | STATS max(metric_1) BY dimension_1
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        EsRelation relation = as(agg.child(), EsRelation.class);
    }

    public void testDimensionsAreNotFiltered() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY dimension_1
            | LIMIT 10
            """);
        Project project = as(actual, Project.class);
        Eval unpack = as(project.child(), Eval.class);
        assertThat(unpack.fields(), hasSize(1));
        assertThat(Alias.unwrap(unpack.fields().get(0)), instanceOf(UnpackDimension.class));
        Limit limit = as(unpack.child(), Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        Eval pack = as(agg.child(), Eval.class);
        assertThat(pack.fields(), hasSize(1));
        assertThat(Alias.unwrap(pack.fields().get(0)), instanceOf(PackDimension.class));
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(pack.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testFiltersAreJoinedWithOr() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
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
        LogicalPlan actual = localPlan("""
            TS test
            | EVAL metric_2 = coalesce(metric_2, 0)
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval eval = as(tsAgg.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    /**
     * check that stats blocks after the first are not sourced for adding metrics to the filter
     */
    public void testMultipleStats() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS m = max(max_over_time(metric_1))
            | STATS sum(m)
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate sumAgg = as(limit.child(), Aggregate.class);
        Aggregate outerAgg = as(sumAgg.child(), Aggregate.class);
        Aggregate tsAgg = as(outerAgg.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }
}
