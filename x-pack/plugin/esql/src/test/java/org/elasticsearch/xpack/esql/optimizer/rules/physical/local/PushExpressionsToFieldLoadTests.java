/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

/**
 * Note: this test does not verify the entire plan, it only looks for the specific changes this rule is meant to apply.
 * For full plan verification, see the golden tests in {@link PushExpressionToFieldLoadGoldenTests}
 */
public class PushExpressionsToFieldLoadTests extends AbstractLocalPhysicalPlanOptimizerTests {
    private TestPlannerOptimizer allTypesPlannerOptimizer;
    private TestPlannerOptimizer tsPlannerOptimizer;

    public PushExpressionsToFieldLoadTests(String name, Configuration config) {
        super(name, config);
    }

    @Before
    public void initPushTests() {
        Analyzer allTypesAnalyzer = EsqlTestUtils.analyzer()
            .configuration(config)
            .addIndex("test_all", "mapping-all-types.json")
            .buildAnalyzer();
        allTypesPlannerOptimizer = new TestPlannerOptimizer(config, allTypesAnalyzer);

        Analyzer tsAnalyzer = EsqlTestUtils.analyzer()
            .configuration(config)
            .addIndex("k8s", "k8s-mappings.json", IndexMode.TIME_SERIES)
            .addIndex("k8s-downsampled", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES)
            .buildAnalyzer();
        tsPlannerOptimizer = new TestPlannerOptimizer(
            config,
            tsAnalyzer,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), TransportVersion.current()))
        );
    }

    // ---- LENGTH push tests ----

    public void testLengthInEval() {
        // The SORT ensures the EVAL is below the exchange boundary (inside the
        // data-node fragment) so the local physical optimizer can push it.
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | EVAL l = LENGTH(last_name)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(1));
    }

    public void testLengthInWhere() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | WHERE LENGTH(last_name) > 1
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(1));

        FilterExec filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec", filterExec);
        GreaterThan gt = as(filterExec.condition(), GreaterThan.class);
        assertLengthPushdown(gt.left(), "last_name");
    }

    public void testLengthInStats() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | STATS l = SUM(LENGTH(last_name))
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(1));

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.child() instanceof FieldAttribute fa) {
                return fa.field() instanceof FunctionEsField fef
                    && fef.functionConfig().function() == BlockLoaderFunctionConfig.Function.LENGTH;
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with LENGTH pushdown", evalExec);
    }

    public void testLengthInEvalAfterManyRenames() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | EVAL l1 = last_name
            | EVAL l2 = l1
            | EVAL l3 = l2
            | EVAL l = LENGTH(l3)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(1));
    }

    public void testLengthInWhereAndEval() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | WHERE LENGTH(last_name) > 1
            | EVAL l = LENGTH(last_name)
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("Duplicate LENGTH(last_name) should be deduplicated to one pushed field", pushed, hasSize(1));
    }

    public void testLengthPushdownZoo() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | EVAL a1 = LENGTH(last_name), a2 = LENGTH(last_name), a3 = LENGTH(last_name),
                   a4 = abs(LENGTH(last_name)) + a1 + LENGTH(first_name) * 3
            | WHERE a1 > 1 and LENGTH(last_name) > 1
            | STATS l = SUM(LENGTH(last_name)) + AVG(a3) + SUM(LENGTH(first_name))
            """);

        List<FieldAttribute> lastNamePushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        List<FieldAttribute> firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);

        assertThat("All LENGTH(last_name) should share one pushed field", lastNamePushed, hasSize(1));
        assertThat("LENGTH(first_name) should have its own pushed field", firstNamePushed, hasSize(1));
    }

    public void testLengthInStatsTwice() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | STATS l = SUM(LENGTH(last_name)) + AVG(LENGTH(last_name))
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("Duplicate LENGTH(last_name) should share one pushed field", pushed, hasSize(1));
    }

    public void testLengthTwoFields() {
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | STATS last_name = SUM(LENGTH(last_name)), first_name = SUM(LENGTH(first_name))
            """);

        List<FieldAttribute> lastNamePushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        List<FieldAttribute> firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);

        assertThat(lastNamePushed, hasSize(1));
        assertThat(firstNamePushed, hasSize(1));
    }

    // ---- ROUND_TO push tests ----

    public void testRoundToInEval() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | EVAL r = ROUND_TO(hire_date, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime)
            | SORT emp_no
            | LIMIT 10
            | KEEP r
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "hire_date", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(1));
    }

    public void testRoundToInWhere() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | WHERE ROUND_TO(hire_date, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime) > "2023-01-01"::datetime
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "hire_date", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(1));

        FilterExec filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec", filterExec);
        GreaterThan gt = as(filterExec.condition(), GreaterThan.class);
        assertRoundToPushdown(gt.left(), "hire_date");
    }

    public void testRoundToInEvalWithLongField() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL r = ROUND_TO(long, 100, 200, 300)
            | SORT integer
            | LIMIT 10
            | KEEP r
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "long", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(1));
    }

    public void testRoundToInEvalAfterManyRenames() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | EVAL h1 = hire_date
            | EVAL h2 = h1
            | EVAL h3 = h2
            | EVAL r = ROUND_TO(h3, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime)
            | SORT emp_no
            | LIMIT 10
            | KEEP r
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "hire_date", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(1));
    }

    public void testRoundToInWhereAndEval() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = plannerOptimizer.plan("""
            FROM test
            | WHERE ROUND_TO(hire_date, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime) > "2023-01-01"::datetime
            | EVAL r = ROUND_TO(hire_date, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime)
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "hire_date", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat("Duplicate ROUND_TO(hire_date, ...) should be deduplicated to one pushed field", pushed, hasSize(1));
    }

    public void testRoundToPushdownZoo() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL a1 = ROUND_TO(long, 100, 200, 300), a2 = ROUND_TO(long, 100, 200, 300), a3 = ROUND_TO(long, 100, 200, 300),
                   a4 = abs(ROUND_TO(long, 100, 200, 300)) + a1,
                   b1 = ROUND_TO(date, "2023-01-01"::date, "2024-01-01"::date)
            | WHERE a1 > 1 AND ROUND_TO(long, 100, 200, 300) > 1
            | SORT integer
            | LIMIT 10
            | KEEP a1, a2, a3, a4, b1
            """);

        List<FieldAttribute> longPushed = findPushedFields(plan, "long", BlockLoaderFunctionConfig.Function.ROUND_TO);
        List<FieldAttribute> datePushed = findPushedFields(plan, "date", BlockLoaderFunctionConfig.Function.ROUND_TO);

        assertThat("All ROUND_TO(long, ...) should share one pushed field", longPushed, hasSize(1));
        assertThat("ROUND_TO(date, ...) should have its own pushed field", datePushed, hasSize(1));
    }

    public void testRoundToInEvalTwice() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL l1 = ROUND_TO(long, 100, 200, 300), l2 = ROUND_TO(long, 100, 200, 300) + 1
            | SORT integer
            | LIMIT 10
            | KEEP l1, l2
            """);

        List<FieldAttribute> pushed = findPushedFields(plan, "long", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat("Duplicate ROUND_TO(long, ...) should share one pushed field", pushed, hasSize(1));
    }

    public void testRoundToTwoFields() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL d = ROUND_TO(date, "2023-01-01"::date, "2024-01-01"::date),
                  l = ROUND_TO(long, 100, 200, 300)
            | SORT integer
            | LIMIT 10
            | KEEP d, l
            """);

        List<FieldAttribute> datePushed = findPushedFields(plan, "date", BlockLoaderFunctionConfig.Function.ROUND_TO);
        List<FieldAttribute> longPushed = findPushedFields(plan, "long", BlockLoaderFunctionConfig.Function.ROUND_TO);

        assertThat(datePushed, hasSize(1));
        assertThat(longPushed, hasSize(1));
    }

    // ---- ROUND_TO push tests (TS mode) ----
    //
    // These tests verify that ROUND_TO is NOT pushed to the block loader in TS
    // mode. The TS command uses a different execution strategy for ROUND_TO
    // (query-and-tags rewrite via {@link ReplaceRoundToWithQueryAndTags}), so the
    // block loader push must be skipped.

    /**
     * Verifies ROUND_TO on a long field is NOT pushed to the block loader in TS mode.
     */
    public void testRoundToInTsEval() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s
            | EVAL r = ROUND_TO(events_received, 100, 200, 300)
            | SORT @timestamp
            | LIMIT 10
            | KEEP r
            """, tsSearchStats());

        List<FieldAttribute> pushed = findPushedFields(plan, "events_received", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(0));
    }

    /**
     * Verifies ROUND_TO on a datetime field ({@code @timestamp}) is NOT pushed
     * to the block loader in TS mode.
     */
    public void testRoundToTimestampInTsEval() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s
            | EVAL r = ROUND_TO(@timestamp, "2023-01-01"::datetime, "2023-06-01"::datetime, "2024-01-01"::datetime)
            | SORT @timestamp
            | LIMIT 10
            | KEEP r
            """, tsSearchStats());

        List<FieldAttribute> pushed = findPushedFields(plan, "@timestamp", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(0));
    }

    /**
     * Verifies ROUND_TO in a WHERE filter is NOT pushed to the block loader in TS mode.
     */
    public void testRoundToInTsWhere() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s
            | WHERE ROUND_TO(events_received, 100, 200, 300) > 100
            """, tsSearchStats());

        List<FieldAttribute> pushed = findPushedFields(plan, "events_received", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(0));
    }

    /**
     * Verifies that ROUND_TO in both WHERE and EVAL is NOT pushed in TS mode.
     */
    public void testRoundToInTsWhereAndEval() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s
            | WHERE ROUND_TO(events_received, 100, 200, 300) > 100
            | EVAL r = ROUND_TO(events_received, 100, 200, 300)
            """, tsSearchStats());

        List<FieldAttribute> pushed = findPushedFields(plan, "events_received", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(0));
    }

    /**
     * Verifies that ROUND_TO on two different fields is NOT pushed in TS mode.
     */
    public void testRoundToTwoFieldsInTs() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s
            | EVAL t = ROUND_TO(@timestamp, "2023-01-01"::datetime, "2024-01-01"::datetime),
                  e = ROUND_TO(events_received, 100, 200, 300)
            | SORT @timestamp
            | LIMIT 10
            | KEEP t, e
            """, tsSearchStats());

        List<FieldAttribute> timestampPushed = findPushedFields(plan, "@timestamp", BlockLoaderFunctionConfig.Function.ROUND_TO);
        List<FieldAttribute> eventsPushed = findPushedFields(plan, "events_received", BlockLoaderFunctionConfig.Function.ROUND_TO);

        assertThat(timestampPushed, hasSize(0));
        assertThat(eventsPushed, hasSize(0));
    }

    /**
     * Verifies ROUND_TO is NOT pushed when using FROM against a time-series index.
     * The check is based on index metadata, not the source command, so FROM queries
     * targeting time-series indices are also skipped.
     */
    public void testRoundToInFromAgainstTimeSeriesIndex() {
        assumeTrue("ROUND_TO block loader must be enabled", EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER.isEnabled());
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            FROM k8s
            | EVAL r = ROUND_TO(events_received, 100, 200, 300)
            | SORT @timestamp
            | LIMIT 10
            | KEEP r
            """, tsSearchStats());

        List<FieldAttribute> pushed = findPushedFields(plan, "events_received", BlockLoaderFunctionConfig.Function.ROUND_TO);
        assertThat(pushed, hasSize(0));
    }

    // ---- Vector function push tests ----

    public void testVectorFunctionsReplaced() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            """, testCase.toQuery()));

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> f.name().equals("s")));
        assertNotNull("Should find EvalExec with field 's'", evalExec);
        Alias sAlias = evalExec.fields().stream().filter(f -> f.name().equals("s")).findFirst().orElseThrow();
        FieldAttribute fieldAttr = as(sAlias.child(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        FunctionEsField field = as(fieldAttr.field(), FunctionEsField.class);
        as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
    }

    public void testVectorFunctionsReplacedWithTopN() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery()));

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("s") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith(testCase.toFieldAttrName());
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed vector function for 's'", evalExec);
    }

    public void testVectorFunctionsInWhere() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | where %s > 0.5
            | keep dense_vector
            """, testCase.toQuery()));

        FilterExec filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec", filterExec);
        GreaterThan gt = as(filterExec.condition(), GreaterThan.class);
        FieldAttribute fieldAttr = as(gt.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        as(fieldAttr.field(), FunctionEsField.class);
    }

    public void testVectorFunctionsInStats() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | stats count(*) where %s > 0.5
            """, testCase.toQuery()));

        FilterExec filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec with pushed vector function", filterExec);
        GreaterThan gt = as(filterExec.condition(), GreaterThan.class);
        FieldAttribute fieldAttr = as(gt.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
    }

    public void testVectorFunctionsUpdateIntermediateProjections() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | keep *
            | mv_expand keyword
            | eval similarity = %s
            | sort similarity desc, keyword asc
            | limit 1
            """, testCase.toQuery()));

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("similarity") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith(testCase.toFieldAttrName());
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed vector function for 'similarity'", evalExec);

        FieldExtractExec fieldExtract = findFirst(
            plan,
            FieldExtractExec.class,
            fe -> fe.attributesToExtract()
                .stream()
                .anyMatch(a -> a instanceof FieldAttribute fa && fa.name().startsWith(testCase.toFieldAttrName()))
        );
        assertNotNull("FieldExtractExec should contain the pushed vector field", fieldExtract);
    }

    public void testVectorFunctionsWithDuplicateFunctions() {
        SimilarityFunctionTestCase testCase1 = SimilarityFunctionTestCase.random("dense_vector");
        SimilarityFunctionTestCase testCase2 = randomValueOtherThan(testCase1, () -> SimilarityFunctionTestCase.random("dense_vector"));
        SimilarityFunctionTestCase testCase3 = randomValueOtherThanMany(
            tc -> (tc.equals(testCase1) || tc.equals(testCase2)),
            () -> SimilarityFunctionTestCase.random("dense_vector")
        );

        // The SORT ensures all EVALs end up inside the data-node fragment.
        PhysicalPlan plan = allTypesPlannerOptimizer.plan(
            String.format(
                Locale.ROOT,
                """
                    from test_all
                    | eval s1 = %s, s2 = %s * 2 / 3
                    | where %s + 5 + %s > 0
                    | eval r2 = %s + %s
                    | sort s1 desc
                    | limit 10
                    | keep s1, s2, r2
                    """,
                testCase1.toQuery(),
                testCase1.toQuery(),
                testCase1.toQuery(),
                testCase2.toQuery(),
                testCase2.toQuery(),
                testCase3.toQuery()
            )
        );

        List<FieldAttribute> allPushed = findAllPushedFields(plan);
        assertThat("Three distinct vector functions should produce three pushed fields", allPushed, hasSize(3));
    }

    // ---- Aggregate Metric Double tests ----

    public void testAggregateMetricDouble() {
        PhysicalPlan plan = tsPlannerOptimizer.plan(
            "FROM k8s-downsampled | STATS m = min(network.eth0.tx)",
            new EsqlTestUtils.TestSearchStats()
        );

        List<FieldAttribute> pushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_MIN);
        assertThat(pushed, hasSize(1));
    }

    public void testAggregateMetricDoubleWithAvgAndOtherFunctions() {
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            from k8s-downsampled
            | STATS s = sum(network.eth0.tx), a = avg(network.eth0.tx)
            """, new EsqlTestUtils.TestSearchStats());

        List<FieldAttribute> sumPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_SUM);
        List<FieldAttribute> countPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_COUNT);
        assertThat(sumPushed, hasSize(1));
        assertThat(countPushed, hasSize(1));
    }

    public void testAggregateMetricDoubleTSCommand() {
        PhysicalPlan plan = tsPlannerOptimizer.plan("""
            TS k8s-downsampled |
            STATS m = max(max_over_time(network.eth0.tx)),
                  c = count(count_over_time(network.eth0.tx)),
                  a = avg(avg_over_time(network.eth0.tx))
            BY pod, time_bucket = BUCKET(@timestamp,5minute)
            """, new EsqlTestUtils.TestSearchStats());

        List<FieldAttribute> maxPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_MAX);
        List<FieldAttribute> countPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_COUNT);
        List<FieldAttribute> sumPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_SUM);
        assertThat(maxPushed, hasSize(1));
        assertThat(countPushed, hasSize(1));
        assertThat(sumPushed, hasSize(1));
    }

    // ---- Reduction plan tests ----

    public void testReductionPlanForTopNWithPushedDownFunctions() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL score = V_DOT_PRODUCT(dense_vector, [1.0, 2.0, 3.0])
            | SORT integer DESC
            | LIMIT 10
            | KEEP text, score
            """);

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("score") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith("$$dense_vector$V_DOT_PRODUCT$");
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed V_DOT_PRODUCT for 'score'", evalExec);
    }

    public void testReductionPlanForTopNWithPushedDownFunctionsInOrder() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL fieldLength = LENGTH(text)
            | SORT fieldLength DESC
            | LIMIT 10
            | KEEP text, fieldLength
            """);

        EvalExec evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("fieldLength") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith("$$text$LENGTH$");
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed LENGTH for 'fieldLength'", evalExec);
    }

    // ---- Fork test ----

    public void testPushableFunctionsInFork() {
        // In the physical plan, fork branch EVALs end up above the exchange
        // boundary (coordinator side), so the local physical optimizer does
        // not see them and cannot push. The data node fragment only contains
        // the raw field extraction. This verifies the plan still builds
        // correctly and the un-pushed expressions are preserved.
        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            from test_all
            | eval u = v_cosine(dense_vector, [4, 5, 6])
            | fork
                (eval s = length(text) | keep s, u, keyword)
                (eval t = v_dot_product(dense_vector, [1, 2, 3]) | keep t, u, keyword)
            | eval x = length(keyword)
            """);

        // The LENGTH(keyword) eval above the fork stays as a raw Length function.
        EvalExec topEval = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            return f.name().equals("x") && f.child() instanceof Length;
        }));
        assertNotNull("LENGTH(keyword) above fork should remain as Length", topEval);

        MergeExec mergeExec = findFirst(plan, MergeExec.class);
        assertNotNull("Plan should contain a MergeExec for fork", mergeExec);
    }

    // ---- Subquery test ----

    public void testPushableFunctionsInSubqueries() {
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled());

        PhysicalPlan plan = allTypesPlannerOptimizer.plan("""
            from test_all, (from test_all | eval s = length(text) | keep s)
            | eval t = v_dot_product(dense_vector, [1, 2, 3])
            | keep s, t
            """);

        boolean foundDotProductAboveUnion = false;
        for (EvalExec node : collectNodes(plan, EvalExec.class)) {
            for (Alias field : node.fields()) {
                if (field.name().equals("t") && field.child() instanceof DotProduct) {
                    foundDotProductAboveUnion = true;
                }
            }
        }
        assertTrue("v_dot_product above union should NOT be pushed (multiple sources)", foundDotProductAboveUnion);

        List<FieldAttribute> textLengthPushed = findPushedFields(plan, "text", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("LENGTH(text) in subquery should be pushed", textLengthPushed, hasSize(1));
    }

    // ---- Lookup join test (Primaries check) ----

    public void testPushDownFunctionsLookupJoin() {
        // The SORT ensures the evals below are in the data-node fragment.
        PhysicalPlan plan = plannerOptimizer.plan("""
            from test
            | eval s = length(first_name)
            | rename languages AS language_code
            | keep s, language_code, last_name
            | lookup join languages_lookup ON language_code
            | eval t = length(last_name)
            | eval u = length(language_name)
            | sort language_code
            | limit 10
            """);

        // "s" (LENGTH(first_name)) is below the join — SHOULD be pushed
        List<FieldAttribute> firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("LENGTH(first_name) below join should be pushed", firstNamePushed, hasSize(1));

        // "t" (LENGTH(last_name)) and "u" (LENGTH(language_name)) are above
        // the join. Because the Primaries check detects multiple sources above
        // the join (main + lookup), these should NOT be pushed.
        LookupJoinExec lookupJoin = findFirst(plan, LookupJoinExec.class);
        assertNotNull("Plan should contain a LookupJoinExec", lookupJoin);

        // Verify that LENGTH expressions above the join are not pushed
        boolean foundUnpushedAboveJoin = false;
        for (EvalExec node : collectNodes(plan, EvalExec.class)) {
            for (Alias field : node.fields()) {
                if ((field.name().equals("t") || field.name().equals("u")) && field.child() instanceof Length) {
                    foundUnpushedAboveJoin = true;
                }
            }
        }
        assertTrue("LENGTH(last_name) and/or LENGTH(language_name) above join should remain as Length", foundUnpushedAboveJoin);
    }

    // ---- Helpers ----

    private record SimilarityFunctionTestCase(String esqlFunction, String fieldName, float[] vector, String functionName) {

        public String toQuery() {
            String params = randomBoolean() ? fieldName + ", " + Arrays.toString(vector) : Arrays.toString(vector) + ", " + fieldName;
            return esqlFunction + "(" + params + ")";
        }

        public String toFieldAttrName() {
            return "$$" + fieldName + "$" + functionName;
        }

        public static SimilarityFunctionTestCase random(String fieldName) {
            float[] vector = new float[] { randomFloat(), randomFloat(), randomFloat() };
            return switch (randomInt(4)) {
                case 0 -> new SimilarityFunctionTestCase("v_dot_product", fieldName, vector, "V_DOT_PRODUCT");
                case 1 -> new SimilarityFunctionTestCase("v_cosine", fieldName, vector, "V_COSINE");
                case 2 -> new SimilarityFunctionTestCase("v_l1_norm", fieldName, vector, "V_L1NORM");
                case 3 -> new SimilarityFunctionTestCase("v_l2_norm", fieldName, vector, "V_L2NORM");
                case 4 -> new SimilarityFunctionTestCase("v_hamming", fieldName, vector, "V_HAMMING");
                default -> throw new IllegalStateException("Unexpected value");
            };
        }
    }

    private Attribute assertLengthPushdown(Expression e, String fieldName) {
        FieldAttribute attr = as(e, FieldAttribute.class);
        assertThat(attr.name(), startsWith("$$" + fieldName + "$LENGTH$"));
        FunctionEsField field = as(attr.field(), FunctionEsField.class);
        assertThat(field.functionConfig().function(), is(BlockLoaderFunctionConfig.Function.LENGTH));
        assertThat(field.getName(), equalTo(fieldName));
        return attr;
    }

    private Attribute assertRoundToPushdown(Expression e, String fieldName) {
        FieldAttribute attr = as(e, FieldAttribute.class);
        assertThat(attr.name(), startsWith("$$" + fieldName + "$ROUND_TO$"));
        FunctionEsField field = as(attr.field(), FunctionEsField.class);
        assertThat(field.functionConfig().function(), is(BlockLoaderFunctionConfig.Function.ROUND_TO));
        assertThat(field.getName(), equalTo(fieldName));
        return attr;
    }

    private List<FieldAttribute> findPushedFields(PhysicalPlan plan, String fieldName, BlockLoaderFunctionConfig.Function function) {
        List<FieldAttribute> result = new ArrayList<>();
        plan.forEachDown(PhysicalPlan.class, node -> {
            node.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field() instanceof FunctionEsField fef
                    && fef.functionConfig().function() == function
                    && fa.fieldName().string().equals(fieldName)) {
                    if (result.stream().noneMatch(existing -> existing.name().equals(fa.name()))) {
                        result.add(fa);
                    }
                }
            });
        });
        return result;
    }

    private List<FieldAttribute> findAllPushedFields(PhysicalPlan plan) {
        List<FieldAttribute> result = new ArrayList<>();
        plan.forEachDown(PhysicalPlan.class, node -> {
            node.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field() instanceof FunctionEsField) {
                    if (result.stream().noneMatch(existing -> existing.name().equals(fa.name()))) {
                        result.add(fa);
                    }
                }
            });
        });
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T extends PhysicalPlan> T findFirst(PhysicalPlan plan, Class<T> type) {
        return findFirst(plan, type, t -> true);
    }

    @SuppressWarnings("unchecked")
    private <T extends PhysicalPlan> T findFirst(PhysicalPlan plan, Class<T> type, java.util.function.Predicate<T> predicate) {
        Holder<T> holder = new Holder<>();
        plan.forEachDown(type, node -> {
            if (holder.get() == null && predicate.test(node)) {
                holder.set(node);
            }
        });
        return holder.get();
    }

    private <T extends PhysicalPlan> List<T> collectNodes(PhysicalPlan plan, Class<T> type) {
        List<T> result = new ArrayList<>();
        plan.forEachDown(type, result::add);
        return result;
    }

    private static SearchStats tsSearchStats() {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public Map<ShardId, IndexMetadata> targetShards() {
                IndexMetadata indexMetadata = IndexMetadata.builder("k8s")
                    .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name()))
                    .build();
                return Map.of(new ShardId(new Index("k8s", "n/a"), 0), indexMetadata);
            }
        };
    }
}
