/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RLike;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.localSource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.ql.TestUtils.relation;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LogicalPlanOptimizerTests extends ESTestCase {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static Map<String, EsField> mapping;
    private static Map<String, EsField> mappingAirports;
    private static Analyzer analyzerAirports;
    private static EnrichResolution enrichResolution;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        enrichResolution = AnalyzerTestUtils.loadEnrichPolicyResolution("languages_idx", "id", "languages_idx", "mapping-languages.json");

        // Most tests used data from the test index, so we load it here, and use it in the plan() function.
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, enrichResolution),
            TEST_VERIFIER
        );

        // Some tests use data from the airports index, so we load it here, and use it in the plan_airports() function.
        mappingAirports = loadMapping("mapping-airports.json");
        EsIndex airports = new EsIndex("airports", mappingAirports, Set.of("airports"));
        IndexResolution getIndexResultAirports = IndexResolution.valid(airports);
        analyzerAirports = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultAirports, enrichResolution),
            TEST_VERIFIER
        );
    }

    public void testEmptyProjections() {
        var plan = plan("""
            from test
            | keep salary
            | drop salary
            """);

        var relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), is(empty()));
        assertThat(relation.supplier().get(), emptyArray());
    }

    public void testEmptyProjectionInStat() {
        var plan = plan("""
            from test
            | stats c = count(salary)
            | drop c
            """);

        var relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), is(empty()));
        assertThat(relation.supplier().get(), emptyArray());
    }

    public void testCombineProjections() {
        var plan = plan("""
            from test
            | keep emp_no, *name, salary
            | keep last_name
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("last_name"));
        var limit = as(keep.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
    }

    public void testCombineProjectionWithFilterInBetween() {
        var plan = plan("""
            from test
            | keep *name, salary
            | where salary > 10
            | keep last_name
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("last_name"));
    }

    public void testCombineProjectionWhilePreservingAlias() {
        var plan = plan("""
            from test
            | rename first_name as x
            | keep x, salary
            | where salary > 10
            | rename x as y
            | keep y
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("y"));
        var p = keep.projections().get(0);
        var alias = as(p, Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("first_name"));
    }

    public void testCombineProjectionWithAggregation() {
        var plan = plan("""
            from test
            | stats s = sum(salary) by last_name, first_name
            | keep s, last_name, first_name
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name"));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    /**
     * Project[[s{r}#4 AS d, s{r}#4, last_name{f}#21, first_name{f}#18]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[last_name{f}#21, first_name{f}#18],[SUM(salary{f}#22) AS s, last_name{f}#21, first_name{f}#18]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testCombineProjectionWithDuplicateAggregation() {
        var plan = plan("""
            from test
            | stats s = sum(salary), d = sum(salary), c = sum(salary) by last_name, first_name
            | keep d, s, last_name, first_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("d", "s", "last_name", "first_name"));
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name"));
        assertThat(Alias.unwrap(agg.aggregates().get(0)), instanceOf(Sum.class));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    public void testQlComparisonOptimizationsApply() {
        var plan = plan("""
            from test
            | where (1 + 4) < salary
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        // The core QL optimizations rotate constants to the right.
        var condition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(condition.left()), equalTo("salary"));
        assertThat(Expressions.name(condition.right()), equalTo("1 + 4"));
        var con = as(condition.right(), Literal.class);
        assertThat(con.value(), equalTo(5));
    }

    public void testCombineProjectionWithPruning() {
        var plan = plan("""
            from test
            | rename first_name as x
            | keep x, salary, last_name
            | stats count(salary) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("count(salary)", "x"));
        assertThat(Expressions.names(agg.groupings()), contains("x"));
        var alias = as(agg.aggregates().get(1), Alias.class);
        var field = as(alias.child(), FieldAttribute.class);
        assertThat(field.name(), is("first_name"));
        var group = as(agg.groupings().get(0), Attribute.class);
        assertThat(group, is(alias.toAttribute()));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[f{r}#7],[SUM(emp_no{f}#15) AS s, COUNT(first_name{f}#16) AS c, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testCombineProjectionWithAggregationFirstAndAliasedGroupingUsedInAgg() {
        var plan = plan("""
            from test
            | rename emp_no as e, first_name as f
            | stats s = sum(e), c = count(f) by f
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("s", "c", "f"));
        Alias as = as(aggs.get(0), Alias.class);
        var sum = as(as.child(), Sum.class);
        assertThat(Expressions.name(sum.field()), is("emp_no"));
        as = as(aggs.get(1), Alias.class);
        var count = as(as.child(), Count.class);
        assertThat(Expressions.name(count.field()), is("first_name"));

        as = as(aggs.get(2), Alias.class);
        assertThat(Expressions.name(as.child()), is("first_name"));

        assertThat(Expressions.names(agg.groupings()), contains("f"));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[f{r}#7],[SUM(emp_no{f}#15) AS s, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testCombineProjectionWithAggregationFirstAndAliasedGroupingUnused() {
        var plan = plan("""
            from test
            | rename emp_no as e, first_name as f, last_name as l
            | stats s = sum(e) by f
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("s", "f"));
        Alias as = as(aggs.get(0), Alias.class);
        var aggFunc = as(as.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.field()), is("emp_no"));
        as = as(aggs.get(1), Alias.class);
        assertThat(Expressions.name(as.child()), is("first_name"));

        assertThat(Expressions.names(agg.groupings()), contains("f"));
    }

    /**
     * Expects
     * EsqlProject[[x{r}#3, y{r}#6]]
     * \_Eval[[emp_no{f}#9 + 2[INTEGER] AS x, salary{f}#14 + 3[INTEGER] AS y]]
     *   \_Limit[10000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testCombineEvals() {
        var plan = plan("""
            from test
            | eval x = emp_no + 2
            | eval y = salary + 3
            | keep x, y
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "y"));
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testCombineLimits() {
        var limitValues = new int[] { randomIntBetween(10, 99), randomIntBetween(100, 1000) };
        var firstLimit = randomBoolean() ? 0 : 1;
        var secondLimit = firstLimit == 0 ? 1 : 0;
        var oneLimit = new Limit(EMPTY, L(limitValues[firstLimit]), emptySource());
        var anotherLimit = new Limit(EMPTY, L(limitValues[secondLimit]), oneLimit);
        assertEquals(
            new Limit(EMPTY, L(Math.min(limitValues[0], limitValues[1])), emptySource()),
            new LogicalPlanOptimizer.PushDownAndCombineLimits().rule(anotherLimit)
        );
    }

    public void testMultipleCombineLimits() {
        var numberOfLimits = randomIntBetween(3, 10);
        var minimum = randomIntBetween(10, 99);
        var limitWithMinimum = randomIntBetween(0, numberOfLimits - 1);

        var fa = getFieldAttribute("a", INTEGER);
        var relation = localSource(TestBlockFactory.getNonBreakingInstance(), singletonList(fa), singletonList(1));
        LogicalPlan plan = relation;

        for (int i = 0; i < numberOfLimits; i++) {
            var value = i == limitWithMinimum ? minimum : randomIntBetween(100, 1000);
            plan = new Limit(EMPTY, L(value), plan);
        }
        assertEquals(
            new Limit(EMPTY, L(minimum), relation),
            new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG)).optimize(plan)
        );
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, randomZone());
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, randomZone());
    }

    public void testCombineFilters() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(
            new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
            new LogicalPlanOptimizer.PushDownAndCombineFilters().apply(fb)
        );
    }

    public void testCombineFiltersLikeRLike() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(
            new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
            new LogicalPlanOptimizer.PushDownAndCombineFilters().apply(fb)
        );
    }

    public void testPushDownFilter() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new LogicalPlanOptimizer.PushDownAndCombineFilters().apply(fb));
    }

    public void testPushDownLikeRlikeFilter() {
        EsRelation relation = relation();
        org.elasticsearch.xpack.ql.expression.predicate.regex.RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new LogicalPlanOptimizer.PushDownAndCombineFilters().apply(fb));
    }

    // from ... | where a > 1 | stats count(1) by b | where count(1) >= 3 and b < 2
    // => ... | where a > 1 and b < 2 | stats count(1) by b | where count(1) >= 3
    public void testSelectivelyPushDownFilterPastFunctionAgg() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        GreaterThanOrEqual aggregateCondition = greaterThanOrEqualOf(new Count(EMPTY, ONE), THREE);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        // invalid aggregate but that's fine cause its properties are not used by this rule
        Aggregate aggregate = new Aggregate(EMPTY, fa, singletonList(getFieldAttribute("b")), emptyList());
        Filter fb = new Filter(EMPTY, aggregate, new And(EMPTY, aggregateCondition, conditionB));

        // expected
        Filter expected = new Filter(
            EMPTY,
            new Aggregate(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                singletonList(getFieldAttribute("b")),
                emptyList()
            ),
            aggregateCondition
        );
        assertEquals(expected, new LogicalPlanOptimizer.PushDownAndCombineFilters().apply(fb));
    }

    public void testSelectivelyPushDownFilterPastRefAgg() {
        // expected plan: "from test | where emp_no > 1 and emp_no < 3 | stats x = count(1) by emp_no | where x > 7"
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no > 1
            | stats x = count(1) by emp_no
            | where x + 2 > 9
            | where emp_no < 3""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof GreaterThan);
        var gt = (GreaterThan) filter.condition();
        assertTrue(gt.left() instanceof ReferenceAttribute);
        var refAttr = (ReferenceAttribute) gt.left();
        assertEquals("x", refAttr.name());
        assertEquals(L(7), gt.right());

        var agg = as(filter.child(), Aggregate.class);

        filter = as(agg.child(), Filter.class);
        assertTrue(filter.condition() instanceof And);
        var and = (And) filter.condition();
        assertTrue(and.left() instanceof GreaterThan);
        gt = (GreaterThan) and.left();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertTrue(and.right() instanceof LessThan);
        var lt = (LessThan) and.right();
        assertTrue(lt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) lt.left()).name());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testNoPushDownOrFilterPastAgg() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats x = count(1) by emp_no
            | where emp_no < 3 or x > 9""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var stats = as(filter.child(), Aggregate.class);
        assertTrue(stats.child() instanceof EsRelation);
    }

    public void testSelectivePushDownComplexFilterPastAgg() {
        // expected plan: from test | emp_no > 0 | stats x = count(1) by emp_no | where emp_no < 3 or x > 9
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats x = count(1) by emp_no
            | where (emp_no < 3 or x > 9) and emp_no > 0""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var stats = as(filter.child(), Aggregate.class);
        filter = as(stats.child(), Filter.class);
        assertTrue(filter.condition() instanceof GreaterThan);
        var gt = (GreaterThan) filter.condition();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertEquals(L(0), gt.right());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSelectivelyPushDownFilterPastEval() {
        // expected plan: "from test | where emp_no > 1 and emp_no < 3 | eval x = emp_no + 1 | where x < 7"
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no > 1
            | eval x = emp_no + 1
            | where x + 2 < 9
            | where emp_no < 3""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof LessThan);
        var lt = (LessThan) filter.condition();
        assertTrue(lt.left() instanceof ReferenceAttribute);
        var refAttr = (ReferenceAttribute) lt.left();
        assertEquals("x", refAttr.name());
        assertEquals(L(7), lt.right());

        var eval = as(filter.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertTrue(eval.fields().get(0) instanceof Alias);
        assertEquals("x", (eval.fields().get(0)).name());

        filter = as(eval.child(), Filter.class);
        assertTrue(filter.condition() instanceof And);
        var and = (And) filter.condition();
        assertTrue(and.left() instanceof GreaterThan);
        var gt = (GreaterThan) and.left();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertTrue(and.right() instanceof LessThan);
        lt = (LessThan) and.right();
        assertTrue(lt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) lt.left()).name());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testNoPushDownOrFilterPastLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 3
            | where emp_no < 3 or salary > 9""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var limit2 = as(filter.child(), Limit.class);
        assertTrue(limit2.child() instanceof EsRelation);
    }

    public void testPushDownFilterPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as x
            | keep x
            | where x > 10""");

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, FieldAttribute.class).name(), is("emp_no"));
    }

    public void testPushDownEvalPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as x
            | keep x
            | eval y = x * 2""");

        var keep = as(plan, Project.class);
        var eval = as(keep.child(), Eval.class);
        assertThat(
            eval.fields(),
            contains(
                new Alias(
                    EMPTY,
                    "y",
                    new Mul(EMPTY, new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")), new Literal(EMPTY, 2, INTEGER))
                )
            )
        );
    }

    public void testPushDownDissectPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | dissect x "%{y}"
            """);

        var keep = as(plan, Project.class);
        var dissect = as(keep.child(), Dissect.class);
        assertThat(dissect.extractedFields(), contains(new ReferenceAttribute(Source.EMPTY, "y", DataTypes.KEYWORD)));
    }

    public void testPushDownGrokPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | grok x "%{WORD:y}"
            """);

        var keep = as(plan, Project.class);
        var grok = as(keep.child(), Grok.class);
        assertThat(grok.extractedFields(), contains(new ReferenceAttribute(Source.EMPTY, "y", DataTypes.KEYWORD)));
    }

    public void testPushDownFilterPastProjectUsingEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval y = emp_no + 1
            | rename y as x
            | where x > 10""");

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testPushDownFilterPastProjectUsingDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | dissect first_name "%{y}"
            | rename y as x
            | keep x
            | where x == "foo"
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var dissect = as(filter.child(), Dissect.class);
        as(dissect.child(), EsRelation.class);
    }

    public void testPushDownFilterPastProjectUsingGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | grok first_name "%{WORD:y}"
            | rename y as x
            | keep x
            | where x == "foo"
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var grok = as(filter.child(), Grok.class);
        as(grok.child(), EsRelation.class);
    }

    public void testPushDownLimitPastEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = emp_no + 100
            | limit 10""");

        var eval = as(plan, Eval.class);
        as(eval.child(), Limit.class);
    }

    public void testPushDownLimitPastDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | dissect first_name "%{y}"
            | limit 10""");

        var dissect = as(plan, Dissect.class);
        as(dissect.child(), Limit.class);
    }

    public void testPushDownLimitPastGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | grok first_name "%{WORD:y}"
            | limit 10""");

        var grok = as(plan, Grok.class);
        as(grok.child(), Limit.class);
    }

    public void testPushDownLimitPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as a
            | keep a
            | limit 10""");

        var keep = as(plan, Project.class);
        as(keep.child(), Limit.class);
    }

    public void testDontPushDownLimitPastFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 100
            | where emp_no > 10
            | limit 10""");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testEliminateHigherLimitDueToDescendantLimit() throws Exception {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 10
            | sort emp_no
            | where emp_no > 10
            | eval c = emp_no + 2
            | limit 100""");

        var topN = as(plan, TopN.class);
        var eval = as(topN.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testDoNotEliminateHigherLimitDueToDescendantLimit() throws Exception {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 10
            | where emp_no > 10
            | stats c = count(emp_no) by emp_no
            | limit 100""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testBasicNullFolding() {
        FoldNull rule = new FoldNull();
        assertNullLiteral(rule.rule(new Add(EMPTY, L(randomInt()), Literal.NULL)));
        assertNullLiteral(rule.rule(new Round(EMPTY, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new Pow(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateFormat(EMPTY, Literal.NULL, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new DateParse(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateTrunc(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new Substring(EMPTY, Literal.NULL, Literal.NULL, Literal.NULL)));
    }

    public void testPruneSortBeforeStats() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | where emp_no > 10
            | stats x = sum(salary) by first_name""");

        var limit = as(plan, Limit.class);
        var stats = as(limit.child(), Aggregate.class);
        var filter = as(stats.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    public void testDontPruneSortWithLimitBeforeStats() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | limit 100
            | stats x = sum(salary) by first_name""");

        var limit = as(plan, Limit.class);
        var stats = as(limit.child(), Aggregate.class);
        var topN = as(stats.child(), TopN.class);
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderBy() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | sort salary""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | eval x = salary + 1
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var eval = as(topN.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughEvalWithTwoDefs() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | eval x = salary + 1, y = salary + 2
            | eval z = x * y
            | sort z""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("z"));
        var eval = as(topN.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "y", "z"));
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | dissect first_name "%{x}"
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var dissect = as(topN.child(), Dissect.class);
        as(dissect.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | grok first_name "%{WORD:x}"
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var grok = as(topN.child(), Grok.class);
        as(grok.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | keep salary, emp_no
            | sort salary""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProjectAndEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename emp_no as en
            | keep salary, en
            | eval e = en * 2
            | sort salary""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        var eval = as(topN.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("e"));
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProjectWithAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename salary as l
            | keep l, emp_no
            | sort l""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | where emp_no > 10
            | sort salary""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        var filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     * TopN[[Order[first_name{f}#170,ASC,LAST]],1000[INTEGER]]
     *  \_MvExpand[first_name{f}#170]
     *    \_TopN[[Order[emp_no{f}#169,ASC,LAST]],1000[INTEGER]]
     *      \_EsRelation[test][avg_worked_seconds{f}#167, birth_date{f}#168, emp_n..]
     */
    public void testDontCombineOrderByThroughMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | sort first_name""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("first_name"));
        var mvExpand = as(topN.child(), MvExpand.class);
        topN = as(mvExpand.child(), TopN.class);
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     *  \_MvExpand[x{r}#159]
     *    \_EsqlProject[[first_name{f}#162 AS x]]
     *      \_Limit[1000[INTEGER]]
     *        \_EsRelation[test][first_name{f}#162]
     */
    public void testCopyDefaultLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | mv_expand x
            """);

        var limit = as(plan, Limit.class);
        var mvExpand = as(limit.child(), MvExpand.class);
        var keep = as(mvExpand.child(), EsqlProject.class);
        var limitPastMvExpand = as(keep.child(), Limit.class);
        assertThat(limitPastMvExpand.limit(), equalTo(limit.limit()));
        as(limitPastMvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[10[INTEGER]]
     *  \_MvExpand[first_name{f}#155]
     *    \_EsqlProject[[first_name{f}#155, last_name{f}#156]]
     *      \_Limit[1[INTEGER]]
     *        \_EsRelation[test][first_name{f}#155, last_name{f}#156]
     */
    public void testDontPushDownLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 1
            | keep first_name, last_name
            | mv_expand first_name
            | limit 10""");

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(10));
        var mvExpand = as(limit.child(), MvExpand.class);
        var project = as(mvExpand.child(), EsqlProject.class);
        limit = as(project.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#141, first_name{f}#142, languages{f}#143, lll{r}#132, salary{f}#147]]
     *  \_TopN[[Order[salary{f}#147,DESC,FIRST], Order[first_name{f}#142,ASC,LAST]],5[INTEGER]]
     *    \_Limit[5[INTEGER]]
     *      \_MvExpand[salary{f}#147]
     *        \_Eval[[languages{f}#143 + 5[INTEGER] AS lll]]
     *          \_Filter[languages{f}#143 > 1[INTEGER]]
     *            \_Limit[10[INTEGER]]
     *              \_MvExpand[first_name{f}#142]
     *                \_TopN[[Order[emp_no{f}#141,DESC,FIRST]],10[INTEGER]]
     *                  \_Filter[emp_no{f}#141 &lt; 10006[INTEGER]]
     *                    \_EsRelation[test][emp_no{f}#141, first_name{f}#142, languages{f}#1..]
     */
    public void testMultipleMvExpandWithSortAndLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no <= 10006
            | sort emp_no desc
            | mv_expand first_name
            | limit 10
            | where languages > 1
            | eval lll = languages + 5
            | mv_expand salary
            | limit 5
            | sort first_name
            | keep emp_no, first_name, languages, lll, salary
            | sort salary desc""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var limit = as(topN.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(5));
        var mvExp = as(limit.child(), MvExpand.class);
        var eval = as(mvExp.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        limit = as(filter.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(10));
        mvExp = as(limit.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10));
        filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#350, first_name{f}#351, salary{f}#352]]
     *  \_TopN[[Order[salary{f}#352,ASC,LAST], Order[first_name{f}#351,ASC,LAST]],5[INTEGER]]
     *    \_MvExpand[first_name{f}#351]
     *      \_TopN[[Order[emp_no{f}#350,ASC,LAST]],10000[INTEGER]]
     *        \_EsRelation[employees][emp_no{f}#350, first_name{f}#351, salary{f}#352]
     */
    public void testPushDownLimitThroughMultipleSort_AfterMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 5""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var mvExp = as(topN.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#361, first_name{f}#362, salary{f}#363]]
     *  \_TopN[[Order[first_name{f}#362,ASC,LAST]],5[INTEGER]]
     *    \_TopN[[Order[salary{f}#363,ASC,LAST]],5[INTEGER]]
     *      \_MvExpand[first_name{f}#362]
     *        \_TopN[[Order[emp_no{f}#361,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#361, first_name{f}#362, salary{f}#363]
     */
    public void testPushDownLimitThroughMultipleSort_AfterMvExpand2() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | sort salary
            | limit 5
            | sort first_name""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("first_name"));
        topN = as(topN.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var mvExp = as(topN.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[5[INTEGER]]
     *  \_Aggregate[[first_name{f}#232],[MAX(salary{f}#233) AS max_s, first_name{f}#232]]
     *    \_Filter[ISNOTNULL(first_name{f}#232)]
     *      \_MvExpand[first_name{f}#232]
     *        \_TopN[[Order[emp_no{f}#231,ASC,LAST]],50[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#231, first_name{f}#232, salary{f}#233]
     */
    public void testDontPushDownLimitPastAggregate_AndMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | limit 50
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | stats max_s = max(salary) by first_name
            | where first_name is not null
            | limit 5""");

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(5));
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        var mvExp = as(filter.child(), MvExpand.class);
        var topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(50));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[5[INTEGER]]
     *  \_Aggregate[[first_name{f}#262],[MAX(salary{f}#263) AS max_s, first_name{f}#262]]
     *    \_Filter[ISNOTNULL(first_name{f}#262)]
     *      \_Limit[50[INTEGER]]
     *        \_MvExpand[first_name{f}#262]
     *          \_Limit[50[INTEGER]]
     *            \_EsRelation[employees][emp_no{f}#261, first_name{f}#262, salary{f}#263]
     */
    public void testPushDown_TheRightLimit_PastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | mv_expand first_name
            | limit 50
            | keep emp_no, first_name, salary
            | stats max_s = max(salary) by first_name
            | where first_name is not null
            | limit 5""");

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(5));
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        limit = as(filter.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(50));
        var mvExp = as(limit.child(), MvExpand.class);
        limit = as(mvExp.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(50));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[first_name{f}#11, emp_no{f}#10, salary{f}#12, b{r}#4]]
     *  \_TopN[[Order[salary{f}#12,ASC,LAST]],5[INTEGER]]
     *    \_Eval[[100[INTEGER] AS b]]
     *      \_MvExpand[first_name{f}#11]
     *        \_TopN[[Order[first_name{f}#11,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#10, first_name{f}#11, salary{f}#12]
     */
    public void testPushDownLimit_PastEvalAndMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort first_name
            | mv_expand first_name
            | eval b = 100
            | sort salary
            | limit 5
            | keep first_name, emp_no, salary, b""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var eval = as(topN.child(), Eval.class);
        var mvExp = as(eval.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("first_name"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#104, first_name{f}#105, salary{f}#106]]
     *  \_TopN[[Order[salary{f}#106,ASC,LAST], Order[first_name{f}#105,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#215 == [46][KEYWORD] AND WILDCARDLIKE(first_name{f}#105)]
     *      \_MvExpand[first_name{f}#105]
     *        \_TopN[[Order[emp_no{f}#104,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#104, first_name{f}#105, salary{f}#106]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilterOnExpandedField() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | where gender == "F"
            | where first_name LIKE "R*"
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 15""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filter acts on first_name (the one used in mv_expand), so the limit 15 is not pushed down past mv_expand
        // instead the default limit is added
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#104, first_name{f}#105, salary{f}#106]]
     *  \_TopN[[Order[salary{f}#106,ASC,LAST], Order[first_name{f}#105,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#215 == [46][KEYWORD] AND salary{f}#106 > 60000[INTEGER]]
     *      \_MvExpand[first_name{f}#105]
     *        \_TopN[[Order[emp_no{f}#104,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#104, first_name{f}#105, salary{f}#106]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilter_NOT_OnExpandedField() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | where gender == "F"
            | where salary > 60000
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 15""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filters after mv_expand do not act on the expanded field values, as such the limit 15 is the one being pushed down
        // otherwise that limit wouldn't have pushed down and the default limit was instead being added by default before mv_expanded
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#116, first_name{f}#117 AS x, salary{f}#119]]
     *  \_TopN[[Order[salary{f}#119,ASC,LAST], Order[first_name{f}#117,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#118 == [46][KEYWORD] AND WILDCARDLIKE(first_name{f}#117)]
     *      \_MvExpand[first_name{f}#117]
     *        \_TopN[[Order[gender{f}#118,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#116, first_name{f}#117, gender{f}#118, sa..]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilterOnExpandedFieldAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort gender
            | mv_expand first_name
            | rename first_name AS x
            | where gender == "F"
            | where x LIKE "A*"
            | keep emp_no, x, salary
            | sort salary, x
            | limit 15""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filter uses an alias ("x") to the expanded field ("first_name"), so the default limit is used and not the one provided
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("gender"));
        as(topN.child(), EsRelation.class);
    }

    private static List<String> orderNames(TopN topN) {
        return topN.order().stream().map(o -> as(o.child(), NamedExpression.class).name()).toList();
    }

    public void testCombineLimitWithOrderByThroughFilterAndEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary
            | eval x = emp_no / 2
            | where x > 20
            | sort x
            | limit 10""");

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testCombineMultipleOrderByAndLimits() {
        // expected plan:
        // from test
        // | sort salary, emp_no
        // | limit 100
        // | where salary > 1
        // | sort emp_no, first_name
        // | keep l = salary, emp_no, first_name
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename salary as l
            | keep l, emp_no, first_name
            | sort l
            | limit 100
            | sort first_name
            | where l > 1
            | sort emp_no""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("emp_no"));
        var filter = as(topN.child(), Filter.class);
        var topN2 = as(filter.child(), TopN.class);
        assertThat(orderNames(topN2), contains("salary"));
        as(topN2.child(), EsRelation.class);
    }

    public void testDontPruneSameFieldDifferentDirectionSortClauses() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary nulls last, emp_no desc nulls first
            | where salary > 2
            | eval e = emp_no * 2
            | keep salary, emp_no, e
            | sort e, emp_no, salary desc, emp_no desc""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(
            topN.order(),
            contains(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, "e", INTEGER, null, Nullability.TRUE, null, false),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "salary", mapping.get("salary")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                )
            )
        );
        assertThat(topN.child().collect(OrderBy.class::isInstance), is(emptyList()));
    }

    public void testPruneRedundantSortClauses() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary desc nulls last, emp_no desc nulls first
            | where salary > 2
            | eval e = emp_no * 2
            | keep salary, emp_no, e
            | sort e, emp_no desc, salary desc, emp_no desc nulls last""");

        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        assertThat(
            topN.order(),
            contains(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, "e", INTEGER, null, Nullability.TRUE, null, false),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "salary", mapping.get("salary")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.LAST
                )
            )
        );
        assertThat(topN.child().collect(OrderBy.class::isInstance), is(emptyList()));
    }

    public void testDontPruneSameFieldDifferentDirectionSortClauses_UsingAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no desc
            | rename emp_no as e
            | keep e
            | sort e""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(
            topN.order(),
            contains(
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                )
            )
        );
    }

    public void testPruneRedundantSortClausesUsingAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no desc
            | rename emp_no as e
            | keep e
            | sort e desc""");

        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        assertThat(
            topN.order(),
            contains(
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                )
            )
        );
    }

    public void testSimplifyLikeNoWildcard() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name like "foo"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Equals);
        Equals equals = as(filter.condition(), Equals.class);
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold());
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyLikeMatchAll() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name like "*"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        as(filter.condition(), IsNotNull.class);
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyRLikeNoWildcard() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name rlike "foo"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Equals);
        Equals equals = as(filter.condition(), Equals.class);
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold());
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyRLikeMatchAll() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name rlike ".*"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        var isNotNull = as(filter.condition(), IsNotNull.class);
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testFoldNullInToLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where null in (first_name, ".*")
            """);
        assertThat(plan, instanceOf(LocalRelation.class));
    }

    public void testFoldNullListInToLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name in (null, null)
            """);
        assertThat(plan, instanceOf(LocalRelation.class));
    }

    public void testFoldInKeyword() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where "foo" in ("bar", "baz")
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where "foo" in ("bar", "foo", "baz")
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInIP() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where to_ip("1.1.1.1") in (to_ip("1.1.1.2"), to_ip("1.1.1.2"))
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where to_ip("1.1.1.1") in (to_ip("1.1.1.1"), to_ip("1.1.1.2"))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInVersion() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where to_version("1.2.3") in (to_version("1"), to_version("1.2.4"))
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where to_version("1.2.3") in (to_version("1"), to_version("1.2.3"))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInNumerics() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where 3 in (4.0, 5, 2147483648)
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where 3 in (4.0, 3.0, to_long(3))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInEval() {
        var plan = optimizedPlan("""
            from test
            | eval a = 1, b = a + 1, c = b + a
            | where c > 10
            """);

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), is(LocalSupplier.EMPTY));
    }

    public void testFoldFromRow() {
        var plan = optimizedPlan("""
              row a = 1, b = 2, c = 3
            | where c > 10
            """);

        as(plan, LocalRelation.class);
    }

    public void testFoldFromRowInEval() {
        var plan = optimizedPlan("""
              row a = 1, b = 2, c = 3
            | eval x = c
            | where x > 10
            """);

        as(plan, LocalRelation.class);
    }

    public void testInvalidFoldDueToReplacement() {
        var plan = optimizedPlan("""
              from test
            | eval x = 1
            | eval x = emp_no
            | where x > 10
            | keep x
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var child = aliased(project.projections().get(0), FieldAttribute.class);
        assertThat(Expressions.name(child), is("emp_no"));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var source = as(filter.child(), EsRelation.class);
    }

    public void testEnrich() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = to_string(languages)
            | enrich languages_idx on x
            """);
        var enrich = as(plan, Enrich.class);
        assertTrue(enrich.policyName().resolved());
        assertThat(enrich.policyName().fold(), is(BytesRefs.toBytesRef("languages_idx")));
        var eval = as(enrich.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testPushDownEnrichPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval a = to_string(languages)
            | rename a as x
            | keep x
            | enrich languages_idx on x
            """);

        var keep = as(plan, Project.class);
        as(keep.child(), Enrich.class);
    }

    public void testTopNEnrich() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename languages as x
            | eval x = to_string(x)
            | keep x
            | enrich languages_idx on x
            | sort language_name
            """);

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        as(topN.child(), Enrich.class);
    }

    public void testEnrichNotNullFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = to_string(languages)
            | enrich languages_idx on x
            | where language_name is not null
            | limit 10
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var enrich = as(filter.child(), Enrich.class);
        assertTrue(enrich.policyName().resolved());
        assertThat(enrich.policyName().fold(), is(BytesRefs.toBytesRef("languages_idx")));
        var eval = as(enrich.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * EsqlProject[[a{r}#3, last_name{f}#9]]
     * \_Eval[[__a_SUM_123{r}#12 / __a_COUNT_150{r}#13 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#9],[SUM(salary{f}#10) AS __a_SUM_123, COUNT(salary{f}#10) AS __a_COUNT_150, last_nam
     * e{f}#9]]
     *       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     */
    public void testSimpleAvgReplacement() {
        var plan = plan("""
              from test
            | stats a = avg(salary) by last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "last_name"));
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        assertThat(a.name(), startsWith("$$SUM$a$"));
        var sum = as(a.child(), Sum.class);

        a = as(aggs.get(1), Alias.class);
        assertThat(a.name(), startsWith("$$COUNT$a$"));
        var count = as(a.child(), Count.class);

        assertThat(Expressions.names(agg.groupings()), contains("last_name"));
    }

    /**
     * Expects
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / c{r}#6 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS c, SUM(salary{f}#16) AS s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testClashingAggAvgReplacement() {
        var plan = plan("""
            from test
            | stats a = avg(salary), c = count(salary), s = sum(salary) by last_name
            """);

        assertThat(Expressions.names(plan.output()), contains("a", "c", "s", "last_name"));
        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c", "s", "last_name"));
    }

    /**
     * Expects
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / __a_COUNT@xxx{r}#18 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS __a_COUNT@xxx, COUNT(languages{f}#14) AS c, SUM(salary{f}#16) AS
     *  s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testSemiClashingAvgReplacement() {
        var plan = plan("""
            from test
            | stats a = avg(salary), c = count(languages), s = sum(salary) by last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "c", "s", "last_name"));
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        assertThat(a.name(), startsWith("$$COUNT$a$0"));
        var sum = as(a.child(), Count.class);

        a = as(aggs.get(1), Alias.class);
        assertThat(a.name(), is("c"));
        var count = as(a.child(), Count.class);

        a = as(aggs.get(2), Alias.class);
        assertThat(a.name(), is("s"));
    }

    /**
     * Expected
     * Limit[10000[INTEGER]]
     * \_Aggregate[[last_name{f}#9],[PERCENTILE(salary{f}#10,50[INTEGER]) AS m, last_name{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     */
    public void testMedianReplacement() {
        var plan = plan("""
              from test
            | stats m = median(salary) by last_name
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("m", "last_name"));
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        var per = as(a.child(), Percentile.class);
        var literal = as(per.percentile(), Literal.class);
        assertThat((int) QuantileStates.MEDIAN, is(literal.fold()));

        assertThat(Expressions.names(agg.groupings()), contains("last_name"));
    }

    public void testSplittingInWithFoldableValue() {
        FieldAttribute fa = getFieldAttribute("foo");
        In in = new In(EMPTY, ONE, List.of(TWO, THREE, fa, L(null)));
        Or expected = new Or(EMPTY, new In(EMPTY, ONE, List.of(TWO, THREE)), new In(EMPTY, ONE, List.of(fa, L(null))));
        assertThat(new LogicalPlanOptimizer.SplitInWithFoldableValue().rule(in), equalTo(expected));
    }

    public void testReplaceFilterWithExact() {
        var plan = plan("""
              from test
            | where job == "foo"
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        FieldAttribute left = as(equals.left(), FieldAttribute.class);
        assertThat(left.name(), equalTo("job"));
    }

    public void testReplaceExpressionWithExact() {
        var plan = plan("""
              from test
            | eval x = job
            """);

        var eval = as(plan, Eval.class);
        var alias = as(eval.fields().get(0), Alias.class);
        var field = as(alias.child(), FieldAttribute.class);
        assertThat(field.name(), equalTo("job"));
    }

    public void testReplaceSortWithExact() {
        var plan = plan("""
              from test
            | sort job
            """);

        var topN = as(plan, TopN.class);
        assertThat(topN.order().size(), equalTo(1));
        var sortField = as(topN.order().get(0).child(), FieldAttribute.class);
        assertThat(sortField.name(), equalTo("job"));
    }

    public void testPruneUnusedEval() {
        var plan = plan("""
              from test
            | eval garbage = salary + 3
            | keep salary
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testPruneChainedEval() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3
            | eval garbage_b = emp_no / garbage_a, garbage_c = garbage_a
            | eval garbage_x = 1 - garbage_b/garbage_c
            | keep salary
            """);
        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#1345) AS c]]
     *   \_EsRelation[test][_meta_field{f}#1346, emp_no{f}#1340, first_name{f}#..]
     */
    public void testPruneEvalDueToStats() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3, x = salary
            | eval garbage_b = x + 3
            | stats c = count(x)
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var aggs = aggregate.aggregates();
        assertThat(Expressions.names(aggs), contains("c"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        var source = as(aggregate.child(), EsRelation.class);
    }

    public void testPruneUnusedAggSimple() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | keep c
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        var aggOne = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggOne.name(), is("c"));
        var count = as(aggOne.child(), Count.class);
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#19) AS x]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     */
    public void testPruneUnusedAggMixedWithEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = c
            | keep x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("x"));
        aggFieldName(agg.aggregates().get(0), Count.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    public void testPruneUnusedAggsChainedAgg() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("c"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[c{r}#342]]
     * \_Limit[1000[INTEGER]]
     *   \_Filter[min{r}#348 > 10[INTEGER]]
     *     \_Aggregate[[],[COUNT(salary{f}#367) AS c, MIN(salary{f}#367) AS min]]
     *       \_EsRelation[test][_meta_field{f}#368, emp_no{f}#362, first_name{f}#36..]
     */
    public void testPruneMixedAggInsideUnusedEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | where y > 10
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var agg = as(filter.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c", "min"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Eval[[max{r}#6 + min{r}#9 + c{r}#3 AS x, min{r}#9 AS y, c{r}#3 AS z]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[COUNT(salary{f}#26) AS c, MAX(salary{f}#26) AS max, MIN(salary{f}#26) AS min]]
     *     \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     */
    public void testNoPruningWhenDealingJustWithEvals() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            """);

        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
    }

    /**
     * Expects
     * Project[[y{r}#6 AS z]]
     * \_Eval[[emp_no{f}#11 + 1[INTEGER] AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testNoPruningWhenChainedEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, y = x + 1, z = y
            | keep z
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("z"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("y"));
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[salary{f}#20 AS x, emp_no{f}#15 AS y]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPruningDuplicateEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | keep x, y
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("x", "y"));
        var child = aliased(projections.get(0), FieldAttribute.class);
        assertThat(child.name(), is("salary"));
        child = aliased(projections.get(1), FieldAttribute.class);
        assertThat(child.name(), is("emp_no"));

        var limit = as(project.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#24) AS cx, COUNT(emp_no{f}#19) AS cy]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     */
    public void testPruneEvalAliasOnAggUngrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cx = count(x), cy = count(y)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cx", "cy"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Count.class, "emp_no");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[x{r}#6],[COUNT(emp_no{f}#17) AS cy, salary{f}#22 AS x]]
     *   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPruneEvalAliasOnAggGroupedByAlias() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cy = count(y) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "x"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        var x = aliased(aggs.get(1), FieldAttribute.class);
        assertThat(x.name(), is("salary"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(emp_no{f}#20) AS cy, MIN(salary{f}#25) AS cx, gender{f}#22]]
     *   \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     */
    public void testPruneEvalAliasOnAggGrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#21],[COUNT(emp_no{f}#19) AS cy, MIN(salary{f}#24) AS cx, gender{f}#21]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     */
    public void testPruneEvalAliasMixedWithRenameOnAggGrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | rename salary as x
            | eval y = emp_no
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testEvalAliasingAcrossCommands() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1
            | eval y = x
            | eval z = y + 1
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "x");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testEvalAliasingInsideSameCommand() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1, y = x, z = y + 1
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "x");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(z{r}#9) AS cy, MIN(x{r}#3) AS cx, gender{f}#22]]
     *   \_Eval[[emp_no{f}#20 + 1[INTEGER] AS x, x{r}#3 + 1[INTEGER] AS z]]
     *     \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     */
    public void testEvalAliasingInsideSameCommandWithShadowing() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1, y = x, z = y + 1, y = z
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "z");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "z"));
        var source = as(eval.child(), EsRelation.class);
    }

    public void testPruneRenameOnAgg() {
        var plan = plan("""
              from test
            | rename emp_no as x
            | rename salary as y
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "emp_no");

        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#14],[COUNT(salary{f}#17) AS cy, MIN(emp_no{f}#12) AS cx, gender{f}#14]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testPruneRenameOnAggBy() {
        var plan = plan("""
              from test
            | rename emp_no as x
            | rename salary as y, gender as g
            | stats cy = count(y), cx = min(x) by g
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "g"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "emp_no");
        var groupby = aliased(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(groupby), is("gender"));

        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[c1{r}#2, c2{r}#4, cs{r}#6, cm{r}#8, cexp{r}#10]]
     * \_Eval[[c1{r}#2 AS c2, c1{r}#2 AS cs, c1{r}#2 AS cm, c1{r}#2 AS cexp]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsCountAll() {
        var plan = plan("""
              from test
            | stats c1 = count(1), c2 = count(2), cs = count(*), cm = count(), cexp = count("123")
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "c2", "cs", "cm", "cexp"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("c2", "cs", "cm", "cexp"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[c1{r}#7, cx{r}#10, cs{r}#12, cy{r}#15]]
     * \_Eval[[c1{r}#7 AS cx, c1{r}#7 AS cs, c1{r}#7 AS cy]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsWithAliasedFields() {
        var plan = plan("""
              from test
            | eval x = 1
            | eval y = x
            | stats c1 = count(1), cx = count(x), cs = count(*), cy = count(y)
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "cx", "cs", "cy"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("cx", "cs", "cy"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[min{r}#1385, max{r}#1388, min{r}#1385 AS min2, max{r}#1388 AS max2, gender{f}#1398]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[gender{f}#1398],[MIN(salary{f}#1401) AS min, MAX(salary{f}#1401) AS max, gender{f}#1398]]
     *     \_EsRelation[test][_meta_field{f}#1402, emp_no{f}#1396, first_name{f}#..]
     */
    public void testEliminateDuplicateAggsMixed() {
        var plan = plan("""
              from test
            | stats min = min(salary), max = max(salary), min2 = min(salary), max2 = max(salary) by gender
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("min", "max", "min2", "max2", "gender"));
        as(projections.get(0), ReferenceAttribute.class);
        as(projections.get(1), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("min"));
        assertThat(Expressions.name(aliased(projections.get(3), ReferenceAttribute.class)), is("max"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("min", "max", "gender"));
        aggFieldName(aggs.get(0), Min.class, "salary");
        aggFieldName(aggs.get(1), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * EsqlProject[[a{r}#5, c{r}#8]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggWithNull() {
        var plan = plan("""
              from test
            | eval x = null + 1
            | stats a = avg(x), c = count(x)
            """);
        fail("Awaits fix");
    }

    /**
     * Expects
     * Project[[max(x){r}#11, max(x){r}#11 AS max(y), max(x){r}#11 AS max(z)]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[MAX(salary{f}#21) AS max(x)]]
     *     \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testEliminateDuplicateAggsNonCount() {
        var plan = plan("""
            from test
            | eval x = salary
            | eval y = x
            | eval z = y
            | stats max(x), max(y), max(z)
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("max(x)", "max(y)", "max(z)"));
        as(projections.get(0), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(1), ReferenceAttribute.class)), is("max(x)"));
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("max(x)"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("max(x)"));
        aggFieldName(aggs.get(0), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[2[INTEGER]]
     * \_Filter[a{r}#6 > 2[INTEGER]]
     *   \_MvExpand[a{r}#2,a{r}#6]
     *     \_Row[[[1, 2, 3][INTEGER] AS a]]
     */
    public void testMvExpandFoldable() {
        LogicalPlan plan = optimizedPlan("""
            row a = [1, 2, 3]
            | mv_expand a
            | where a > 2
            | limit 2""");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var expand = as(filter.child(), MvExpand.class);
        assertThat(filter.condition(), instanceOf(GreaterThan.class));
        var filterProp = ((GreaterThan) filter.condition()).left();
        assertTrue(expand.expanded().semanticEquals(filterProp));
        assertFalse(expand.target().semanticEquals(filterProp));
        var row = as(expand.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#2],[COUNT([2a][KEYWORD]) AS bar]]
     *   \_Row[[1[INTEGER] AS a]]
     */
    public void testRenameStatsDropGroup() {
        LogicalPlan plan = optimizedPlan("""
            row a = 1
            | rename a AS foo
            | stats bar = count(*) by foo
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("a"));
        var row = as(agg.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#2, bar{r}#8],[COUNT([2a][KEYWORD]) AS baz, b{r}#4 AS bar]]
     *   \_Row[[1[INTEGER] AS a, 2[INTEGER] AS b]]
     */
    public void testMultipleRenameStatsDropGroup() {
        LogicalPlan plan = optimizedPlan("""
            row a = 1, b = 2
            | rename a AS foo, b as bar
            | stats baz = count(*) by foo, bar
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("a", "bar"));
        var row = as(agg.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#11, bar{r}#4],[MAX(salary{f}#16) AS baz, gender{f}#13 AS bar]]
     *   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testMultipleRenameStatsDropGroupMultirow() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no AS foo, gender as bar
            | stats baz = max(salary) by foo, bar
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("emp_no", "bar"));
        var row = as(agg.child(), EsRelation.class);
    }

    public void testLimitZeroUsesLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats count=count(*)
            | sort count desc
            | limit 0""");

        assertThat(plan, instanceOf(LocalRelation.class));
    }

    private <T> T aliased(Expression exp, Class<T> clazz) {
        var alias = as(exp, Alias.class);
        return as(alias.child(), clazz);
    }

    private <T extends AggregateFunction> void aggFieldName(Expression exp, Class<T> aggType, String fieldName) {
        var alias = as(exp, Alias.class);
        var af = as(alias.child(), aggType);
        var field = af.field();
        var name = field.foldable() ? BytesRefs.toString(field.fold()) : Expressions.name(field);
        assertThat(name, is(fieldName));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#4) AS sum(emp_no)]]
     *   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     */
    public void testIsNotNullConstraintForStatsWithoutGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(emp_no)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(empty()));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(emp_no)"));
        var from = as(agg.child(), EsRelation.class);
    }

    public void testIsNotNullConstraintForStatsWithGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(emp_no) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(emp_no)", "salary"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#1185],[SUM(salary{f}#1185) AS sum(salary), salary{f}#1185]]
     *   \_EsRelation[test][_meta_field{f}#1186, emp_no{f}#1180, first_name{f}#..]
     */
    public void testIsNotNullConstraintForStatsWithAndOnGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(salary) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(salary)", "salary"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[x{r}#4],[SUM(salary{f}#13) AS sum(salary), salary{f}#13 AS x]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testIsNotNullConstraintForStatsWithAndOnGroupingAlias() {
        var plan = optimizedPlan("""
            from test
            | eval x = salary
            | stats sum(salary) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("x"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(salary)", "x"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#13],[SUM(emp_no{f}#8) AS sum(x), salary{f}#13]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testIsNotNullConstraintSkippedForStatsWithAlias() {
        var plan = optimizedPlan("""
            from test
            | eval x = emp_no
            | stats sum(x) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(x)", "salary"));

        // non null filter for stats
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#8) AS a, MIN(salary{f}#13) AS b]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithoutGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#11],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, gender{f}#11]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b", "gender"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#9],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, emp_no{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithAndOnGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b", "emp_no"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[w{r}#14, g{r}#16],[COUNT(b{r}#24) AS c, w{r}#14, gender{f}#32 AS g]]
     *   \_Eval[[emp_no{f}#30 / 10[INTEGER] AS x, x{r}#4 + salary{f}#35 AS y, y{r}#8 / 4[INTEGER] AS z, z{r}#11 * 2[INTEGER] +
     *  3[INTEGER] AS w, salary{f}#35 + 4[INTEGER] / 2[INTEGER] AS a, a{r}#21 + 3[INTEGER] AS b]]
     *     \_EsRelation[test][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
     */
    public void testIsNotNullConstraintForAliasedExpressions() {
        var plan = optimizedPlan("""
            from test
            | eval x = emp_no / 10
            | eval y = x + salary
            | eval z = y / 4
            | eval w = z * 2 + 3
            | rename gender as g, salary as s
            | eval a = (s + 4) / 2
            | eval b = a + 3
            | stats c = count(b) by w, g
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("c", "w", "g"));
        var eval = as(agg.child(), Eval.class);
        var from = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     *   \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]
     */
    public void testSpatialTypesAndStatsUseDocValues() {
        var plan = planAirports("""
            from test
            | stats centroid = st_centroid(location)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("centroid"));
        assertTrue("Expected GEO_POINT aggregation for STATS", agg.aggregates().stream().allMatch(aggExp -> {
            var alias = as(aggExp, Alias.class);
            var aggFunc = as(alias.child(), AggregateFunction.class);
            var aggField = as(aggFunc.field(), FieldAttribute.class);
            return aggField.dataType() == GEO_POINT;
        }));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#6],[COUNT(salary{f}#12) AS c, emp_no%2{r}#6]]
     *   \_Eval[[emp_no{f}#7 % 2[INTEGER] AS emp_no%2]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testNestedExpressionsInGroups() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary) by emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        var aggs = agg.aggregates();
        var ref = as(groupings.get(0), ReferenceAttribute.class);
        assertThat(aggs.get(1), is(ref));
        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        assertThat(eval.fields().get(0).toAttribute(), is(ref));
        assertThat(eval.fields().get(0).name(), is("emp_no % 2"));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#6],[COUNT(__c_COUNT@1bd45f36{r}#16) AS c, emp_no{f}#6]]
     *   \_Eval[[salary{f}#11 + 1[INTEGER] AS __c_COUNT@1bd45f36]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testNestedExpressionsInAggs() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary + 1) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var count = aliased(aggs.get(0), Count.class);
        var ref = as(count.field(), ReferenceAttribute.class);
        var eval = as(agg.child(), Eval.class);
        var fields = eval.fields();
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).toAttribute(), is(ref));
        var add = aliased(fields.get(0), Add.class);
        assertThat(Expressions.name(add.left()), is("salary"));
    }

    /**
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#7],[COUNT(__c_COUNT@fb7855b0{r}#18) AS c, emp_no%2{r}#7]]
     *   \_Eval[[emp_no{f}#8 % 2[INTEGER] AS emp_no%2, 100[INTEGER] / languages{f}#11 + salary{f}#13 + 1[INTEGER] AS __c_COUNT
     * @fb7855b0]]
     *     \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testNestedExpressionsInBothAggsAndGroups() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary + 1 + 100 / languages) by emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        var aggs = agg.aggregates();
        var gRef = as(groupings.get(0), ReferenceAttribute.class);
        assertThat(aggs.get(1), is(gRef));

        var count = aliased(aggs.get(0), Count.class);
        var aggRef = as(count.field(), ReferenceAttribute.class);
        var eval = as(agg.child(), Eval.class);
        var fields = eval.fields();
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).toAttribute(), is(gRef));
        assertThat(fields.get(1).toAttribute(), is(aggRef));

        var mod = aliased(fields.get(0), Mod.class);
        assertThat(Expressions.name(mod.left()), is("emp_no"));
        var refs = Expressions.references(singletonList(fields.get(1)));
        assertThat(Expressions.names(refs), containsInAnyOrder("languages", "salary"));
    }

    public void testNestedMultiExpressionsInGroupingAndAggs() {
        var plan = optimizedPlan("""
            from test
            | stats count(salary + 1), max(salary   +  23) by languages   + 1, emp_no %  3
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.output()), contains("count(salary + 1)", "max(salary   +  23)", "languages   + 1", "emp_no %  3"));
    }

    public void testLogicalPlanOptimizerVerifier() {
        var plan = plan("""
            from test
            | eval bucket_start = 1, bucket_end = 100000
            | eval auto_bucket(salary, 10, bucket_start, bucket_end)
            """);
        var ab = as(plan, Eval.class);
        assertTrue(ab.optimized());
    }

    public void testLogicalPlanOptimizerVerificationException() {
        VerificationException e = expectThrows(VerificationException.class, () -> plan("""
            from test
            | eval bucket_end = 100000
            | eval auto_bucket(salary, 10, emp_no, bucket_end)
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        assertEquals(
            "3:32: third argument of [auto_bucket(salary, 10, emp_no, bucket_end)] must be a constant, received [emp_no]",
            e.getMessage().substring(header.length())
        );
    }

    /**
     * Expects
     * Project[[x{r}#5]]
     * \_Eval[[____x_AVG@9efc3cf3_SUM@daf9f221{r}#18 / ____x_AVG@9efc3cf3_COUNT@53cd08ed{r}#19 AS __x_AVG@9efc3cf3, __x_AVG@
     * 9efc3cf3{r}#16 / 2[INTEGER] + __x_MAX@475d0e4d{r}#17 AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[SUM(salary{f}#11) AS ____x_AVG@9efc3cf3_SUM@daf9f221, COUNT(salary{f}#11) AS ____x_AVG@9efc3cf3_COUNT@53cd0
     * 8ed, MAX(salary{f}#11) AS __x_MAX@475d0e4d]]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testStatsExpOverAggs() {
        var plan = optimizedPlan("""
            from test
            | stats x = avg(salary) /2 + max(salary)
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.name(fields.get(1)), is("x"));
        // sum/count to compute avg
        var div = as(fields.get(0).child(), Div.class);
        // avg + max
        var add = as(fields.get(1).child(), Add.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(3));
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        assertThat(Expressions.name(sum.field()), is("salary"));
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        assertThat(Expressions.name(count.field()), is("salary"));
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        assertThat(Expressions.name(max.field()), is("salary"));
    }

    /**
     * Expects
     * Project[[x{r}#5, y{r}#9, z{r}#12]]
     * \_Eval[[$$SUM$$$AVG$avg(salary_%_3)>$0$0{r}#29 / $$COUNT$$$AVG$avg(salary_%_3)>$0$1{r}#30 AS $$AVG$avg(salary_%_3)>$0,
     *   $$AVG$avg(salary_%_3)>$0{r}#23 + $$MAX$avg(salary_%_3)>$1{r}#24 AS x,
     *   $$MIN$min(emp_no_/_3)>$2{r}#25 + 10[INTEGER] - $$MEDIAN$min(emp_no_/_3)>$3{r}#26 AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[z{r}#12],[SUM($$salary_%_3$AVG$0{r}#27) AS $$SUM$$$AVG$avg(salary_%_3)>$0$0,
     *     COUNT($$salary_%_3$AVG$0{r}#27) AS $$COUNT$$$AVG$avg(salary_%_3)>$0$1,
     *     MAX(emp_no{f}#13) AS $$MAX$avg(salary_%_3)>$1,
     *     MIN($$emp_no_/_3$MIN$1{r}#28) AS $$MIN$min(emp_no_/_3)>$2,
     *     PERCENTILE(salary{f}#18,50[INTEGER]) AS $$MEDIAN$min(emp_no_/_3)>$3, z{r}#12]]
     *       \_Eval[[languages{f}#16 % 2[INTEGER] AS z,
     *       salary{f}#18 % 3[INTEGER] AS $$salary_%_3$AVG$0,
     *       emp_no{f}#13 / 3[INTEGER] AS $$emp_no_/_3$MIN$1]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testStatsExpOverAggsMulti() {
        var plan = optimizedPlan("""
            from test
            | stats x = avg(salary % 3) + max(emp_no), y = min(emp_no / 3) + 10 - median(salary) by z = languages % 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x", "y", "z"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // avg + max
        assertThat(Expressions.name(fields.get(1)), containsString("x"));
        assertThat(Alias.unwrap(fields.get(1)), instanceOf(Add.class));
        // min + 10 - median
        assertThat(Expressions.name(fields.get(2)), containsString("y"));
        assertThat(Alias.unwrap(fields.get(2)), instanceOf(Sub.class));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);
        var percentile = as(Alias.unwrap(aggs.get(4)), Percentile.class);

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("z"));
        assertThat(Expressions.name(fields.get(1)), containsString("AVG"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(1))), containsString("salary"));
        assertThat(Expressions.name(fields.get(2)), containsString("MIN"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(2))), containsString("emp_no"));
    }

    /**
     * Expects
     * Project[[x{r}#5, y{r}#9, z{r}#12]]
     * \_Eval[[$$SUM$$$AVG$CONCAT(TO_STRIN>$0$0{r}#29 / $$COUNT$$$AVG$CONCAT(TO_STRIN>$0$1{r}#30 AS $$AVG$CONCAT(TO_STRIN>$0,
     *        CONCAT(TOSTRING($$AVG$CONCAT(TO_STRIN>$0{r}#23),TOSTRING($$MAX$CONCAT(TO_STRIN>$1{r}#24)) AS x,
     *        $$MIN$(MIN(emp_no_/_3>$2{r}#25 + 3.141592653589793[DOUBLE] - $$MEDIAN$(MIN(emp_no_/_3>$3{r}#26 / 2.718281828459045[DOUBLE]
     *         AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[z{r}#12],[SUM($$salary_%_3$AVG$0{r}#27) AS $$SUM$$$AVG$CONCAT(TO_STRIN>$0$0,
     *      COUNT($$salary_%_3$AVG$0{r}#27) AS $$COUNT$$$AVG$CONCAT(TO_STRIN>$0$1,
     *      MAX(emp_no{f}#13) AS $$MAX$CONCAT(TO_STRIN>$1,
     *      MIN($$emp_no_/_3$MIN$1{r}#28) AS $$MIN$(MIN(emp_no_/_3>$2,
     *      PERCENTILE(salary{f}#18,50[INTEGER]) AS $$MEDIAN$(MIN(emp_no_/_3>$3, z{r}#12]]
     *       \_Eval[[languages{f}#16 % 2[INTEGER] AS z,
     *       salary{f}#18 % 3[INTEGER] AS $$salary_%_3$AVG$0,
     *       emp_no{f}#13 / 3[INTEGER] AS $$emp_no_/_3$MIN$1]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testStatsExpOverAggsWithScalars() {
        var plan = optimizedPlan("""
            from test
            | stats x = CONCAT(TO_STRING(AVG(salary % 3)), TO_STRING(MAX(emp_no))),
                    y = (MIN(emp_no / 3) + PI() - MEDIAN(salary))/E()
                    by z = languages % 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x", "y", "z"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // concat(to_string(avg)
        assertThat(Expressions.name(fields.get(1)), containsString("x"));
        var concat = as(Alias.unwrap(fields.get(1)), Concat.class);
        var toString = as(concat.children().get(0), ToString.class);
        toString = as(concat.children().get(1), ToString.class);
        // min + 10 - median/e
        assertThat(Expressions.name(fields.get(2)), containsString("y"));
        assertThat(Alias.unwrap(fields.get(2)), instanceOf(Div.class));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);
        var percentile = as(Alias.unwrap(aggs.get(4)), Percentile.class);
        assertThat(Expressions.name(aggs.get(5)), is("z"));

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("z"));
        assertThat(Expressions.name(fields.get(1)), containsString("AVG"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(1))), containsString("salary"));
        assertThat(Expressions.name(fields.get(2)), containsString("MIN"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(2))), containsString("emp_no"));
    }

    /**
     * Expects
     * Project[[a{r}#5, b{r}#9, $$max(salary)_+_3>$COUNT$2{r}#46 AS d, $$count(salary)_->$MIN$3{r}#47 AS e, $$avg(salary)_+_m
     * >$MAX$1{r}#45 AS g]]
     * \_Eval[[$$$$avg(salary)_+_m>$AVG$0$SUM$0{r}#48 / $$max(salary)_+_3>$COUNT$2{r}#46 AS $$avg(salary)_+_m>$AVG$0, $$avg(
     * salary)_+_m>$AVG$0{r}#44 + $$avg(salary)_+_m>$MAX$1{r}#45 AS a, $$avg(salary)_+_m>$MAX$1{r}#45 + 3[INTEGER] +
     * 3.141592653589793[DOUBLE] + $$max(salary)_+_3>$COUNT$2{r}#46 AS b]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[w{r}#28],[SUM(salary{f}#39) AS $$$$avg(salary)_+_m>$AVG$0$SUM$0, MAX(salary{f}#39) AS $$avg(salary)_+_m>$MAX$1
     * , COUNT(salary{f}#39) AS $$max(salary)_+_3>$COUNT$2, MIN(salary{f}#39) AS $$count(salary)_->$MIN$3]]
     *       \_Eval[[languages{f}#37 % 2[INTEGER] AS w]]
     *         \_EsRelation[test][_meta_field{f}#40, emp_no{f}#34, first_name{f}#35, ..]
     */
    public void testStatsExpOverAggsWithScalarAndDuplicateAggs() {
        var plan = optimizedPlan("""
            from test
            | stats a = avg(salary) + max(salary),
                    b = max(salary) + 3 + PI() + count(salary),
                    c = count(salary) - min(salary),
                    d = count(salary),
                    e = min(salary),
                    f = max(salary),
                    g = max(salary)
                    by w = languages % 2
            | keep a, b, d, e, g
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("a", "b", "d", "e", "g"));
        var refA = Alias.unwrap(projections.get(0));
        var refB = Alias.unwrap(projections.get(1));
        var refD = Alias.unwrap(projections.get(2));
        var refE = Alias.unwrap(projections.get(3));
        var refG = Alias.unwrap(projections.get(4));

        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // avg + max
        assertThat(Expressions.name(fields.get(1)), is("a"));
        var add = as(Alias.unwrap(fields.get(1)), Add.class);
        var max_salary = add.right();
        assertThat(Expressions.attribute(fields.get(1)), is(Expressions.attribute(refA)));

        assertThat(Expressions.name(fields.get(2)), is("b"));
        assertThat(Expressions.attribute(fields.get(2)), is(Expressions.attribute(refB)));

        add = as(Alias.unwrap(fields.get(2)), Add.class);
        add = as(add.left(), Add.class);
        add = as(add.left(), Add.class);
        assertThat(Expressions.attribute(max_salary), is(Expressions.attribute(add.left())));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);

        assertThat(Expressions.attribute(aggs.get(1)), is(Expressions.attribute(max_salary)));
        var max = as(Alias.unwrap(aggs.get(1)), Max.class);
        var count = as(Alias.unwrap(aggs.get(2)), Count.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("w"));
    }

    /**
     * Expects
     * Project[[a{r}#5, a{r}#5 AS b, w{r}#12]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[w{r}#12],[SUM($$salary_/_2_+_la>$SUM$0{r}#26) AS a, w{r}#12]]
     *     \_Eval[[emp_no{f}#16 % 2[INTEGER] AS w, salary{f}#21 / 2[INTEGER] + languages{f}#19 AS $$salary_/_2_+_la>$SUM$0]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testStatsWithCanonicalAggregate() throws Exception {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(salary / 2 + languages),
                    b = sum(languages + salary / 2)
                    by w = emp_no % 2
            | keep a, b, w
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "b", "w"));
        assertThat(Expressions.name(Alias.unwrap(project.projections().get(1))), is("a"));
        var limit = as(project.child(), Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var aggregates = aggregate.aggregates();
        assertThat(Expressions.names(aggregates), contains("a", "w"));
        var unwrapped = Alias.unwrap(aggregates.get(0));
        var sum = as(unwrapped, Sum.class);
        var sum_argument = sum.field();
        var grouping = aggregates.get(1);

        var eval = as(aggregate.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.attribute(fields.get(0)), is(Expressions.attribute(grouping)));
        assertThat(Expressions.attribute(fields.get(1)), is(Expressions.attribute(sum_argument)));
    }

    public void testEmptyMappingIndex() {
        EsIndex empty = new EsIndex("empty_test", emptyMap(), emptySet());
        IndexResolution getIndexResultAirports = IndexResolution.valid(empty);
        var analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultAirports, enrichResolution),
            TEST_VERIFIER
        );

        var plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test")));
        as(plan, LocalRelation.class);
        assertThat(plan.output(), equalTo(NO_FIELDS));

        plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test metadata _id | eval x = 1")));
        as(plan, LocalRelation.class);
        assertThat(Expressions.names(plan.output()), contains("_id", "x"));

        plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test metadata _id, _version | limit 5")));
        as(plan, LocalRelation.class);
        assertThat(Expressions.names(plan.output()), contains("_id", "_version"));

        plan = logicalOptimizer.optimize(
            analyzer.analyze(parser.createStatement("from empty_test | eval x = \"abc\" | enrich languages_idx on x"))
        );
        LocalRelation local = as(plan, LocalRelation.class);
        assertThat(Expressions.names(local.output()), contains(NO_FIELDS.get(0).name(), "x", "language_code", "language_name"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105436")
    public void testPlanSanityCheck() throws Exception {
        var plan = optimizedPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        // emulate a rule that adds an invalid field
        var invalidPlan = new OrderBy(
            limit.source(),
            limit,
            asList(
                new Order(
                    limit.source(),
                    salary,
                    org.elasticsearch.xpack.ql.expression.Order.OrderDirection.ASC,
                    org.elasticsearch.xpack.ql.expression.Order.NullsPosition.FIRST
                )
            )
        );

        VerificationException e = expectThrows(VerificationException.class, () -> logicalOptimizer.optimize(invalidPlan));
        assertThat(e.getMessage(), containsString("Plan [OrderBy[[Order[salary"));
        assertThat(e.getMessage(), containsString(" optimized incorrectly due to missing references [salary"));
    }

    /**
     * Pushing down EVAL/GROK/DISSECT/ENRICH must not accidentally shadow attributes required by SORT.
     *
     * For DISSECT expects the following; the others are similar.
     *
     * EsqlProject[[first_name{f}#37, emp_no{r}#33, salary{r}#34]]
     * \_TopN[[Order[$$emp_no$temp_name$36{r}#46 + $$salary$temp_name$41{r}#47 * 13[INTEGER],ASC,LAST], Order[NEG($$salary$t
     * emp_name$41{r}#47),DESC,FIRST]],3[INTEGER]]
     *   \_Dissect[first_name{f}#37,Parser[pattern=%{emp_no} %{salary}, appendSeparator=, parser=org.elasticsearch.dissect.Dissect
     * Parser@b6858b],[emp_no{r}#33, salary{r}#34]]
     *     \_Eval[[emp_no{f}#36 AS $$emp_no$temp_name$36, salary{f}#41 AS $$salary$temp_name$41]]
     *       \_EsRelation[test][_meta_field{f}#42, emp_no{f}#36, first_name{f}#37, ..]
     */
    public void testPushdownWithOverwrittenName() {
        List<String> overwritingCommands = List.of(
            "EVAL emp_no = 3*emp_no, salary = -2*emp_no-salary",
            "DISSECT first_name \"%{emp_no} %{salary}\"",
            "GROK first_name \"%{WORD:emp_no} %{WORD:salary}\"",
            "ENRICH languages_idx ON first_name WITH emp_no = language_code, salary = language_code"
        );

        String queryTemplateKeepAfter = """
            FROM test
            | SORT 13*(emp_no+salary) ASC, -salary DESC
            | {}
            | KEEP first_name, emp_no, salary
            | LIMIT 3
            """;
        // Equivalent but with KEEP first - ensures that attributes in the final projection are correct after pushdown rules were applied.
        String queryTemplateKeepFirst = """
            FROM test
            | KEEP emp_no, salary, first_name
            | SORT 13*(emp_no+salary) ASC, -salary DESC
            | {}
            | LIMIT 3
            """;

        for (String overwritingCommand : overwritingCommands) {
            String queryTemplate = randomBoolean() ? queryTemplateKeepFirst : queryTemplateKeepAfter;
            var plan = optimizedPlan(LoggerMessageFormat.format(null, queryTemplate, overwritingCommand));

            var project = as(plan, Project.class);
            var projections = project.projections();
            assertThat(projections.size(), equalTo(3));
            assertThat(projections.get(0).name(), equalTo("first_name"));
            assertThat(projections.get(1).name(), equalTo("emp_no"));
            assertThat(projections.get(2).name(), equalTo("salary"));

            var topN = as(project.child(), TopN.class);
            assertThat(topN.order().size(), is(2));

            var firstOrderExpr = as(topN.order().get(0), Order.class);
            var mul = as(firstOrderExpr.child(), Mul.class);
            var add = as(mul.left(), Add.class);
            var renamed_emp_no = as(add.left(), ReferenceAttribute.class);
            var renamed_salary = as(add.right(), ReferenceAttribute.class);
            assertThat(renamed_emp_no.toString(), startsWith("$$emp_no$temp_name"));
            assertThat(renamed_salary.toString(), startsWith("$$salary$temp_name"));

            var secondOrderExpr = as(topN.order().get(1), Order.class);
            var neg = as(secondOrderExpr.child(), Neg.class);
            var renamed_salary2 = as(neg.field(), ReferenceAttribute.class);
            assert (renamed_salary2.semanticEquals(renamed_salary) && renamed_salary2.equals(renamed_salary));

            Eval renamingEval = null;
            if (overwritingCommand.startsWith("EVAL")) {
                // Multiple EVALs should be merged, so there's only one.
                renamingEval = as(topN.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("DISSECT")) {
                var dissect = as(topN.child(), Dissect.class);
                renamingEval = as(dissect.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("GROK")) {
                var grok = as(topN.child(), Grok.class);
                renamingEval = as(grok.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("ENRICH")) {
                var enrich = as(topN.child(), Enrich.class);
                renamingEval = as(enrich.child(), Eval.class);
            }

            AttributeSet attributesCreatedInEval = new AttributeSet();
            for (Alias field : renamingEval.fields()) {
                attributesCreatedInEval.add(field.toAttribute());
            }
            assert (attributesCreatedInEval.contains(renamed_emp_no));
            assert (attributesCreatedInEval.contains(renamed_salary));

            assertThat(renamingEval.child(), instanceOf(EsRelation.class));
        }
    }

    private LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    private LogicalPlan plan(String query) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    private LogicalPlan planAirports(String query) {
        var analyzed = analyzerAirports.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
    }

    // TODO: move these from org.elasticsearch.xpack.ql.optimizer.OptimizerRulesTests to org.elasticsearch.xpack.ql.TestUtils
    private static FieldAttribute getFieldAttribute(String name) {
        return getFieldAttribute(name, INTEGER);
    }

    private static FieldAttribute getFieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", dataType, emptyMap(), true));
    }

    public static WildcardLike wildcardLike(Expression left, String exp) {
        return new WildcardLike(EMPTY, left, new WildcardPattern(exp));
    }

    public static RLike rlike(Expression left, String exp) {
        return new RLike(EMPTY, left, new RLikePattern(exp));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
