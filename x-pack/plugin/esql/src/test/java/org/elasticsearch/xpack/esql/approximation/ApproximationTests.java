/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.Result;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class ApproximationTests extends ESTestCase {

    private static final EsqlParser parser = EsqlParser.INSTANCE;
    private static final LogicalPlanPreOptimizer preOptimizer = new LogicalPlanPreOptimizer(
        new LogicalPreOptimizerContext(FoldContext.small(), mock(InferenceService.class), TransportVersion.current())
    );
    private static final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
    private static final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
    private static final MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

    /**
     * Runner that simulates the execution of an ESQL query.
     * <p>
     * The runner always returns a result with one field: the number of rows.
     * <p>
     * The runner is initialized with a total number of rows (returned when
     * there's no WHERE or MV_EXPAND in the query), and a number of stats rows
     * (returned otherwise). If there's random sampling in the query, the
     * returned number of rows is multiplied by the sampling probability.
     * <p>
     * The runner also provides the LogicalPlan to PhysicalPlan conversion,
     * but it does not return a realistic PhysicalPlan. When running a
     * PhysicalPlan is invoked, it maps it back to the original LogicalPlan,
     * because LogicalPlans are easier to analyze in tests.
     * <p>
     * The runner collects the LogicalPlans of its invocations.
     */
    private static class TestRunner implements Function<LogicalPlan, PhysicalPlan>, EsqlSession.PlanRunner {

        private final long sourceRows;  // Number of rows in the source data
        private final long statsRows;  // Number of rows arriving at the STATS command
        private final List<LogicalPlan> invocations;
        private final Map<PhysicalPlan, LogicalPlan> toLogicalPlan = new HashMap<>();

        static ActionListener<Result> resultCloser = ActionListener.wrap(result -> result.pages().getFirst().close(), ESTestCase::fail);

        TestRunner(long sourceRows, long statsRows) {
            this.sourceRows = sourceRows;
            this.statsRows = statsRows;
            this.invocations = new ArrayList<>();
        }

        @Override
        public PhysicalPlan apply(LogicalPlan logicalPlan) {
            // Return a dummy PhysicalPlan that can be mapped back to the LogicalPlan.
            PhysicalPlan physicalPlan = new LocalSourceExec(
                Source.EMPTY,
                List.of(new ReferenceAttribute(Source.EMPTY, null, "id", null)),
                EmptyLocalSupplier.EMPTY
            );
            toLogicalPlan.put(physicalPlan, logicalPlan);
            return physicalPlan;
        }

        @Override
        public void run(
            PhysicalPlan physicalPlan,
            Configuration configuration,
            FoldContext foldContext,
            PlanTimeProfile planTimeProfile,
            ActionListener<Result> listener
        ) {
            LogicalPlan logicalPlan = toLogicalPlan.get(physicalPlan);
            invocations.add(logicalPlan);
            List<LogicalPlan> filtersAndMvExpands = logicalPlan.collect(plan -> plan instanceof Filter || plan instanceof MvExpand);
            long numResults = filtersAndMvExpands.isEmpty() ? sourceRows : statsRows;
            List<LogicalPlan> samples = logicalPlan.collect(plan -> plan instanceof Sample);
            for (LogicalPlan sample : samples) {
                numResults = (long) (numResults * (double) ((Literal) ((Sample) sample).probability()).value());
            }
            LongBlock block = blockFactory.newConstantLongBlockWith(numResults, 1);
            listener.onResponse(new Result(null, List.of(new Page(block)), null, null, null));
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testVerify_validQuery() throws Exception {
        verify("FROM test | WHERE emp_no<99 | SORT last_name | MV_EXPAND salary | STATS COUNT() BY gender");
        verify("FROM test | CHANGE_POINT salary ON emp_no | EVAL x=1 | DROP emp_no | STATS SUM(salary) BY x");
        verify("FROM test | LIMIT 1000 | KEEP gender, emp_no | RENAME gender AS whatever | STATS MEDIAN(emp_no)");
        verify("FROM test | EVAL blah=1 | GROK last_name \"%{IP:x}\" | SAMPLE 0.1 | STATS a=COUNT() | LIMIT 100 | SORT a");
        verify("ROW i=[1,2,3] | EVAL x=TO_STRING(i) | DISSECT x \"%{x}\" | STATS i=10*POW(PERCENTILE(i, 0.5), 2) | LIMIT 10");
        verify("FROM test | URI_PARTS parts = last_name | STATS scheme_count = COUNT() BY parts.scheme | LIMIT 10");
    }

    public void testVerify_validQuery_queryProperties() throws Exception {
        assertThat(
            verify("FROM test | SORT last_name | RENAME gender AS whatever | EVAL blah=1 | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, false))
        );
        assertThat(
            verify("FROM test | STATS COUNT() BY emp_no | WHERE emp_no > 10 | LIMIT 3 | MV_EXPAND emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, false))
        );
        assertThat(
            verify("FROM test | WHERE emp_no > 10 | STATS SUM(emp_no)"),
            equalTo(new Approximation.QueryProperties(false, true, false))
        );
        assertThat(
            verify("FROM test | LIMIT 3 | STATS COUNT(), SUM(emp_no) BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, true, false))
        );
        assertThat(
            verify("FROM test | SAMPLE 0.3 | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, true, false))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, true))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | LIMIT 42 | STATS COUNT()"),
            equalTo(new Approximation.QueryProperties(false, true, true))
        );
    }

    public void testVerify_exactlyOneStats() {
        assertError(
            "FROM test | EVAL x = 1 | SORT emp_no | LIMIT 100 | MV_EXPAND x",
            equalTo("line 1:1: query without [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | STATS COUNT()",
            equalTo("line 1:39: query with multiple [STATS] cannot be approximated")
        );
    }

    public void testVerify_incompatibleSourceCommand() {
        assertError("SHOW INFO | STATS COUNT()", equalTo("line 1:1: query with [SHOW INFO] cannot be approximated"));
        assertError("TS k8s | STATS COUNT(network.cost)", equalTo("line 1:1: query with [TS k8s] cannot be approximated"));
    }

    public void testVerify_incompatibleProcessingCommand() {
        assertError(
            "FROM test | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:29: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM test | INLINE STATS COUNT() | STATS COUNT()",
            equalTo("line 1:13: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | INLINE STATS COUNT()",
            equalTo("line 1:29: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM test | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: query with [LOOKUP JOIN test_lookup ON emp_no] cannot be approximated")
        );
        assertError(
            "FROM test | STATS emp_no=COUNT() | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:36: query with [LOOKUP JOIN test_lookup ON emp_no] cannot be approximated")
        );
    }

    public void testVerify_incompatibleAggregation() {
        assertError(
            "FROM test | SORT emp_no | STATS MIN(emp_no) | LIMIT 100",
            equalTo("line 1:33: aggregation function [MIN(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS SUM(emp_no), VALUES(emp_no), TOP(emp_no, 2, \"ASC\"), COUNT()",
            equalTo("line 1:32: aggregation function [VALUES(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS 5+10*POW(MAX(emp_no), 2) BY gender",
            equalTo("line 1:28: aggregation function [MAX(emp_no)] cannot be approximated")
        );
    }

    public void testCountPlan_noData() throws Exception {
        TestRunner runner = new TestRunner(0, 0);
        Approximation approximation = createApproximation("FROM test | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which is zero)
        // - one pass to execute the original query
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(not(hasSample()), hasSum()));
    }

    public void testCountPlan_largeDataNoFilters() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximation approximation = createApproximation("FROM test | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasSample(1e-4), hasSum()));
    }

    public void testCountPlan_smallDataNoFilters() throws Exception {
        TestRunner runner = new TestRunner(1_000, 1_000);
        Approximation approximation = createApproximation("FROM test | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which is small)
        // - one pass to execute the original query
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(not(hasSample()), hasSum()));
    }

    public void testCountPlan_largeDataAfterFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000_000L, 1_000_000_000);
        Approximation approximation = createApproximation("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs four passes:
        // - one pass to get the total number of rows
        // - two passes to get the number of filtered rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(4));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("emp_no"), hasSample(1e-8), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasFilter("emp_no"), hasSample(1e-5), not(hasSum())));
        assertThat(runner.invocations.get(3), allOf(hasFilter("emp_no"), hasSample(1e-4), hasSum()));
    }

    public void testCountPlan_smallDataAfterFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000_000_000_000L, 100);
        Approximation approximation = createApproximation("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs six passes:
        // - one pass to get the total number of rows
        // - four passes to get the number of filtered rows (which is small)
        // - one pass to execute the original query
        assertThat(runner.invocations, hasSize(6));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("emp_no"), hasSample(1e-14), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasFilter("emp_no"), hasSample(1e-10), not(hasSum())));
        assertThat(runner.invocations.get(3), allOf(hasFilter("emp_no"), hasSample(1e-6), not(hasSum())));
        assertThat(runner.invocations.get(4), allOf(hasFilter("emp_no"), hasSample(1e-2), not(hasSum())));
        assertThat(runner.invocations.get(5), allOf(hasFilter("emp_no"), not(hasSample()), hasSum()));
    }

    public void testCountPlan_smallDataBeforeFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000, 10);
        Approximation approximation = createApproximation("FROM test | WHERE gender == \"X\" | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which is small)
        // - one pass to execute the original query
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("gender")), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("gender"), not(hasSample()), hasSum()));
    }

    public void testCountPlan_smallDataAfterMvExpanding() throws Exception {
        TestRunner runner = new TestRunner(1_000, 10_000);
        Approximation approximation = createApproximation("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs three passes:
        // - one pass to get the total number of rows
        // - one pass to get the number of expanded rows (which determines the sample probability)
        // - one pass to execute the original query
        assertThat(runner.invocations, hasSize(3));
        assertThat(runner.invocations.get(0), allOf(not(hasMvExpand("emp_no")), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasMvExpand("emp_no"), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasMvExpand("emp_no"), not(hasSample()), hasSum()));
    }

    public void testCountPlan_largeDataAfterMvExpanding() throws Exception {
        TestRunner runner = new TestRunner(1_000, 1_000_000_000);
        Approximation approximation = createApproximation("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs three passes:
        // - one pass to get the total number of rows
        // - one pass to get the number of expanded rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(3));
        assertThat(runner.invocations.get(0), allOf(not(hasMvExpand("emp_no")), not(hasSample()), not(hasSum()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasMvExpand("emp_no"), not(hasSample()), not(hasSum()), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasMvExpand("emp_no"), hasSample(1e-4), hasSum()));
    }

    public void testCountPlan_largeDataBeforeMvExpanding() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000_000L);
        Approximation approximation = createApproximation("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs three passes:
        // - one pass to get the total number of rows
        // - one pass to sample the number of expanded rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(3));
        assertThat(runner.invocations.get(0), allOf(not(hasMvExpand("emp_no")), not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasMvExpand("emp_no"), hasSample(1e-5), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasMvExpand("emp_no"), hasSample(1e-7), hasSum()));
    }

    public void testCountPlan_sampleProbabilityThreshold_noFilter() throws Exception {
        TestRunner runner = new TestRunner(500_000, 500_000);
        Approximation approximation = createApproximation("FROM test | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows
        // - one pass to execute the original query (because the sample probability is 20%)
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasSample()), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(not(hasSample()), hasSum()));
    }

    public void testCountPlan_sampleProbabilityThreshold_withFilter() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000_000L, 200_000);
        Approximation approximation = createApproximation("FROM test | WHERE emp_no > 1 | STATS SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs four passes:
        // - one pass to get the total number of rows
        // - two passes to get the number of filtered rows (which determines the sample probability)
        // - one pass to execute the original query (because the sample probability is 50%)
        assertThat(runner.invocations, hasSize(4));
        assertThat(runner.invocations.get(0), allOf(not(hasSample()), not(hasFilter("emp_no")), not(hasSum())));
        assertThat(runner.invocations.get(1), allOf(hasSample(1e-8), hasFilter("emp_no"), not(hasSum())));
        assertThat(runner.invocations.get(2), allOf(hasSample(1e-4), hasFilter("emp_no"), not(hasSum())));
        assertThat(runner.invocations.get(3), allOf(not(hasSample()), hasFilter("emp_no"), hasSum()));
    }

    public void testApproximationPlan_createsConfidenceInterval_withoutGrouping() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximation approximation = createApproximation("FROM test | STATS COUNT(), SUM(emp_no)", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.getLast();
        assertThat(approximatePlan, hasSample(1e-4));
        // Counting all rows is exact, so no confidence interval is output.
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(COUNT())")));
        assertThat(approximatePlan, not(hasEval("CERTIFIED(COUNT())")));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(SUM(emp_no))"));
        assertThat(approximatePlan, hasEval("CERTIFIED(SUM(emp_no))"));
    }

    public void testApproximationPlan_createsConfidenceInterval_withGrouping() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximation approximation = createApproximation("FROM test | STATS COUNT(), SUM(emp_no) BY emp_no", runner);
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.getLast();
        assertThat(approximatePlan, hasSample(1e-3));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(COUNT())"));
        assertThat(approximatePlan, hasEval("CERTIFIED(COUNT())"));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(SUM(emp_no))"));
        assertThat(approximatePlan, hasEval("CERTIFIED(SUM(emp_no))"));
    }

    public void testApproximationPlan_dependentConfidenceIntervals() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximation approximation = createApproximation(
            "FROM test | STATS x=SUM(emp_no) | EVAL a=x*x, b=7, c=TO_STRING(x), d=MV_APPEND(x, 1::LONG), e=a+POW(b, 2)",
            runner
        );
        approximation.approximate(TestRunner.resultCloser);
        // This plan needs two passes:
        // - one pass to get the total number of rows (which determines the sample probability)
        // - one pass to approximate the query
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.getLast();
        assertThat(approximatePlan, hasPlan(Sample.class, s -> Foldables.literalValueOf(s.probability()).equals(1e-4)));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(x)"));
        assertThat(approximatePlan, hasEval("CERTIFIED(x)"));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(a)"));
        assertThat(approximatePlan, hasEval("CERTIFIED(a)"));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(b)")));
        assertThat(approximatePlan, not(hasEval("CERTIFIED(b)")));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(c)")));
        assertThat(approximatePlan, not(hasEval("CERTIFIED(c)")));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(d)")));
        assertThat(approximatePlan, not(hasEval("CERTIFIED(d)")));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(e)"));
        assertThat(approximatePlan, hasEval("CERTIFIED(e)"));
    }

    private Matcher<? super LogicalPlan> hasFilter(String field) {
        return hasPlan(
            Filter.class,
            filter -> filter.condition().anyMatch(expr -> expr instanceof NamedExpression ne && ne.name().equals(field))
        );
    }

    private Matcher<? super LogicalPlan> hasMvExpand(String field) {
        return hasPlan(MvExpand.class, mvExpand -> mvExpand.target().name().equals(field));
    }

    private Matcher<? super LogicalPlan> hasEval(String field) {
        return hasPlan(Eval.class, eval -> eval.fields().stream().anyMatch(alias -> alias.name().equals(field)));
    }

    private Matcher<? super LogicalPlan> hasSample() {
        return hasPlan(Sample.class, sample -> true);
    }

    private Matcher<? super LogicalPlan> hasSample(Double probability) {
        return hasPlan(Sample.class, sample -> sample.probability().equals(Literal.fromDouble(Source.EMPTY, probability)));
    }

    private Matcher<? super LogicalPlan> hasSum() {
        return hasPlan(Aggregate.class, aggr -> aggr.aggregates().stream().anyMatch(named -> named.anyMatch(expr -> expr instanceof Sum)));
    }

    private <E extends LogicalPlan> Matcher<? super LogicalPlan> hasPlan(Class<E> typeToken, Predicate<? super E> predicate) {
        return new TypeSafeMatcher<>() {
            @Override
            @SuppressWarnings("unchecked")
            protected boolean matchesSafely(LogicalPlan logicalPlan) {
                return logicalPlan.anyMatch(plan -> plan.getClass() == typeToken && predicate.test((E) plan));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a plan containing [" + typeToken.getSimpleName() + "] matching the predicate");
            }
        };
    }

    private void assertError(String esql, Matcher<String> matcher) {
        Exception e = assertThrows(VerificationException.class, () -> verify(esql));
        assertThat(e.getMessage().substring("Found 1 problem\n".length()), matcher);
    }

    private Approximation.QueryProperties verify(String query) throws Exception {
        return Approximation.verifyPlan(getLogicalPlan(query));
    }

    private Approximation createApproximation(String query, TestRunner runner) throws Exception {
        return new Approximation(
            getLogicalPlan(query),
            ApproximationSettings.DEFAULT,
            mock(EsqlExecutionInfo.class),
            runner,
            runner,
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            TransportVersionUtils.randomCompatibleVersion(),
            new PlanTimeProfile()
        );
    }

    private LogicalPlan getLogicalPlan(String query) throws Exception {
        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        LogicalPlan plan = parser.createStatement(query, new QueryParams()).plan();
        plan = AnalyzerTestUtils.defaultAnalyzer().analyze(plan);
        plan.setAnalyzed();
        preOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        return resultHolder.get();
    }
}
