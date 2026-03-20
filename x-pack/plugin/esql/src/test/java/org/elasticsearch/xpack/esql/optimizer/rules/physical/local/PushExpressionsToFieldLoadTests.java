/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
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
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
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
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class PushExpressionsToFieldLoadTests extends AbstractLocalPhysicalPlanOptimizerTests {
    private TestPlannerOptimizer allTypesPlannerOptimizer;
    private TestPlannerOptimizer tsPlannerOptimizer;

    public PushExpressionsToFieldLoadTests(String name, Configuration config) {
        super(name, config);
    }

    @Before
    public void initPushTests() {
        var allTypesMapping = loadMapping("mapping-all-types.json");
        var allTypesIndex = EsIndexGenerator.esIndex("test_all", allTypesMapping, Map.of("test_all", IndexMode.STANDARD));
        allTypesPlannerOptimizer = new TestPlannerOptimizer(config, makeAnalyzer(IndexResolution.valid(allTypesIndex)));

        var tsMapping = loadMapping("k8s-mappings.json");
        var tsIndex = EsIndexGenerator.esIndex("k8s", tsMapping, Map.of("k8s", IndexMode.TIME_SERIES));
        var tsDownsampledMapping = loadMapping("k8s-downsampled-mappings.json");
        var tsDownsampledIndex = EsIndexGenerator.esIndex(
            "k8s-downsampled",
            tsDownsampledMapping,
            Map.of("k8s-downsampled", IndexMode.TIME_SERIES)
        );
        var tsAnalyzer = new Analyzer(
            testAnalyzerContext(
                config,
                TEST_FUNCTION_REGISTRY,
                indexResolutions(tsIndex, tsDownsampledIndex),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(TEST_FUNCTION_REGISTRY, true, true), new XPackLicenseState(() -> 0L))
        );
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
        var plan = plannerOptimizer.plan("""
            FROM test
            | EVAL l = LENGTH(last_name)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(greaterThanOrEqualTo(1)));
    }

    public void testLengthInWhere() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | WHERE LENGTH(last_name) > 1
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(greaterThanOrEqualTo(1)));

        var filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec", filterExec);
        var gt = as(filterExec.condition(), GreaterThan.class);
        assertLengthPushdown(gt.left(), "last_name");
    }

    public void testLengthInStats() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | STATS l = SUM(LENGTH(last_name))
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(greaterThanOrEqualTo(1)));

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.child() instanceof FieldAttribute fa) {
                return fa.field() instanceof FunctionEsField fef
                    && fef.functionConfig().function() == BlockLoaderFunctionConfig.Function.LENGTH;
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with LENGTH pushdown", evalExec);
    }

    public void testLengthInEvalAfterManyRenames() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | EVAL l1 = last_name
            | EVAL l2 = l1
            | EVAL l3 = l2
            | EVAL l = LENGTH(l3)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat(pushed, hasSize(greaterThanOrEqualTo(1)));
    }

    public void testLengthInWhereAndEval() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | WHERE LENGTH(last_name) > 1
            | EVAL l = LENGTH(last_name)
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("Duplicate LENGTH(last_name) should be deduplicated to one pushed field", pushed, hasSize(1));
    }

    public void testLengthPushdownZoo() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | EVAL a1 = LENGTH(last_name), a2 = LENGTH(last_name), a3 = LENGTH(last_name),
                   a4 = abs(LENGTH(last_name)) + a1 + LENGTH(first_name) * 3
            | WHERE a1 > 1 and LENGTH(last_name) > 1
            | STATS l = SUM(LENGTH(last_name)) + AVG(a3) + SUM(LENGTH(first_name))
            """);

        var lastNamePushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        var firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);

        assertThat("All LENGTH(last_name) should share one pushed field", lastNamePushed, hasSize(1));
        assertThat("LENGTH(first_name) should have its own pushed field", firstNamePushed, hasSize(1));
    }

    public void testLengthInStatsTwice() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | STATS l = SUM(LENGTH(last_name)) + AVG(LENGTH(last_name))
            """);

        var pushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("Duplicate LENGTH(last_name) should share one pushed field", pushed, hasSize(1));
    }

    public void testLengthTwoFields() {
        var plan = plannerOptimizer.plan("""
            FROM test
            | STATS last_name = SUM(LENGTH(last_name)), first_name = SUM(LENGTH(first_name))
            """);

        var lastNamePushed = findPushedFields(plan, "last_name", BlockLoaderFunctionConfig.Function.LENGTH);
        var firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);

        assertThat(lastNamePushed, hasSize(1));
        assertThat(firstNamePushed, hasSize(1));
    }

    // ---- Vector function push tests ----

    public void testVectorFunctionsReplaced() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        var plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            """, testCase.toQuery()));

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> f.name().equals("s")));
        assertNotNull("Should find EvalExec with field 's'", evalExec);
        var sAlias = evalExec.fields().stream().filter(f -> f.name().equals("s")).findFirst().orElseThrow();
        var fieldAttr = as(sAlias.child(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
    }

    public void testVectorFunctionsReplacedWithTopN() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        var plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery()));

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("s") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith(testCase.toFieldAttrName());
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed vector function for 's'", evalExec);
    }

    public void testVectorFunctionsInWhere() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        var plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | where %s > 0.5
            | keep dense_vector
            """, testCase.toQuery()));

        var filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec", filterExec);
        var gt = as(filterExec.condition(), GreaterThan.class);
        var fieldAttr = as(gt.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        as(fieldAttr.field(), FunctionEsField.class);
    }

    public void testVectorFunctionsInStats() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        var plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | stats count(*) where %s > 0.5
            """, testCase.toQuery()));

        var filterExec = findFirst(plan, FilterExec.class);
        assertNotNull("Should find FilterExec with pushed vector function", filterExec);
        var gt = as(filterExec.condition(), GreaterThan.class);
        var fieldAttr = as(gt.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
    }

    public void testVectorFunctionsUpdateIntermediateProjections() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        var plan = allTypesPlannerOptimizer.plan(String.format(Locale.ROOT, """
            from test_all
            | keep *
            | mv_expand keyword
            | eval similarity = %s
            | sort similarity desc, keyword asc
            | limit 1
            """, testCase.toQuery()));

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("similarity") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith(testCase.toFieldAttrName());
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed vector function for 'similarity'", evalExec);

        var fieldExtract = findFirst(
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
        var plan = allTypesPlannerOptimizer.plan(
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

        var allPushed = findAllPushedFields(plan);
        assertThat("Three distinct vector functions should produce three pushed fields", allPushed, hasSize(3));
    }

    // ---- Aggregate Metric Double tests ----

    public void testAggregateMetricDouble() {
        var plan = tsPlannerOptimizer.plan("FROM k8s-downsampled | STATS m = min(network.eth0.tx)", new EsqlTestUtils.TestSearchStats());

        var pushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_MIN);
        assertThat(pushed, hasSize(greaterThanOrEqualTo(1)));
    }

    public void testAggregateMetricDoubleWithAvgAndOtherFunctions() {
        var plan = tsPlannerOptimizer.plan("""
            from k8s-downsampled
            | STATS s = sum(network.eth0.tx), a = avg(network.eth0.tx)
            """, new EsqlTestUtils.TestSearchStats());

        var sumPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_SUM);
        var countPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_COUNT);
        assertThat(sumPushed, hasSize(greaterThanOrEqualTo(1)));
        assertThat(countPushed, hasSize(greaterThanOrEqualTo(1)));
    }

    public void testAggregateMetricDoubleTSCommand() {
        var plan = tsPlannerOptimizer.plan("""
            TS k8s-downsampled |
            STATS m = max(max_over_time(network.eth0.tx)),
                  c = count(count_over_time(network.eth0.tx)),
                  a = avg(avg_over_time(network.eth0.tx))
            BY pod, time_bucket = BUCKET(@timestamp,5minute)
            """, new EsqlTestUtils.TestSearchStats());

        var maxPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_MAX);
        var countPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_COUNT);
        var sumPushed = findPushedFields(plan, "network.eth0.tx", BlockLoaderFunctionConfig.Function.AMD_SUM);
        assertThat(maxPushed, hasSize(greaterThanOrEqualTo(1)));
        assertThat(countPushed, hasSize(greaterThanOrEqualTo(1)));
        assertThat(sumPushed, hasSize(greaterThanOrEqualTo(1)));
    }

    // ---- Reduction plan tests ----

    public void testReductionPlanForTopNWithPushedDownFunctions() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        var plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL score = V_DOT_PRODUCT(dense_vector, [1.0, 2.0, 3.0])
            | SORT integer DESC
            | LIMIT 10
            | KEEP text, score
            """);

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            if (f.name().equals("score") && f.child() instanceof FieldAttribute fa) {
                return fa.name().startsWith("$$dense_vector$V_DOT_PRODUCT$");
            }
            return false;
        }));
        assertNotNull("Should find EvalExec with pushed V_DOT_PRODUCT for 'score'", evalExec);
    }

    public void testReductionPlanForTopNWithPushedDownFunctionsInOrder() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        var plan = allTypesPlannerOptimizer.plan("""
            FROM test_all
            | EVAL fieldLength = LENGTH(text)
            | SORT fieldLength DESC
            | LIMIT 10
            | KEEP text, fieldLength
            """);

        var evalExec = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
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
        var plan = allTypesPlannerOptimizer.plan("""
            from test_all
            | eval u = v_cosine(dense_vector, [4, 5, 6])
            | fork
                (eval s = length(text) | keep s, u, keyword)
                (eval t = v_dot_product(dense_vector, [1, 2, 3]) | keep t, u, keyword)
            | eval x = length(keyword)
            """);

        // The LENGTH(keyword) eval above the fork stays as a raw Length function.
        var topEval = findFirst(plan, EvalExec.class, e -> e.fields().stream().anyMatch(f -> {
            return f.name().equals("x") && f.child() instanceof Length;
        }));
        assertNotNull("LENGTH(keyword) above fork should remain as Length", topEval);

        var mergeExec = findFirst(plan, MergeExec.class);
        assertNotNull("Plan should contain a MergeExec for fork", mergeExec);
    }

    // ---- Subquery test ----

    public void testPushableFunctionsInSubqueries() {
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled());

        var plan = allTypesPlannerOptimizer.plan("""
            from test_all, (from test_all | eval s = length(text) | keep s)
            | eval t = v_dot_product(dense_vector, [1, 2, 3])
            | keep s, t
            """);

        boolean foundDotProductAboveUnion = false;
        for (var node : collectNodes(plan, EvalExec.class)) {
            for (Alias field : node.fields()) {
                if (field.name().equals("t") && field.child() instanceof DotProduct) {
                    foundDotProductAboveUnion = true;
                }
            }
        }
        assertTrue("v_dot_product above union should NOT be pushed (multiple sources)", foundDotProductAboveUnion);

        var textLengthPushed = findPushedFields(plan, "text", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("LENGTH(text) in subquery should be pushed", textLengthPushed, hasSize(greaterThanOrEqualTo(1)));
    }

    // ---- Lookup join test (Primaries check) ----

    public void testPushDownFunctionsLookupJoin() {
        // The SORT ensures the evals below are in the data-node fragment.
        var plan = plannerOptimizer.plan("""
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
        var firstNamePushed = findPushedFields(plan, "first_name", BlockLoaderFunctionConfig.Function.LENGTH);
        assertThat("LENGTH(first_name) below join should be pushed", firstNamePushed, hasSize(greaterThanOrEqualTo(1)));

        // "t" (LENGTH(last_name)) and "u" (LENGTH(language_name)) are above
        // the join. Because the Primaries check detects multiple sources above
        // the join (main + lookup), these should NOT be pushed.
        var lookupJoin = findFirst(plan, LookupJoinExec.class);
        assertNotNull("Plan should contain a LookupJoinExec", lookupJoin);

        // Verify that LENGTH expressions above the join are not pushed
        boolean foundUnpushedAboveJoin = false;
        for (var node : collectNodes(plan, EvalExec.class)) {
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
        var field = as(attr.field(), FunctionEsField.class);
        assertThat(field.functionConfig().function(), is(BlockLoaderFunctionConfig.Function.LENGTH));
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
}
