/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

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
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.ConfidenceInterval;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.Double.NaN;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class ApproximateTests extends ESTestCase {

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
     * there are no filters in the query), and a number of filtered rows
     * (returned when there are filters in the query). If there's random
     * sampling in the query, the returned number of rows is multiplied by
     * the sampling probability.
     * <p>
     * The runner also provides the LogicalPlan to PhysicalPlan conversion,
     * but it does not return a realistic PhysicalPlan. When running a
     * PhysicalPlan is invoked, it maps it back to the original LogicalPlan,
     * because LogicalPlans are easier to analyze in tests.
     * <p>
     * The runner collects the LogicalPlans of its invocations.
     */
    private static class TestRunner implements Function<LogicalPlan, PhysicalPlan>, EsqlSession.PlanRunner {

        private final long totalRows;
        private final long filteredRows;
        private final List<LogicalPlan> invocations;
        private final Map<PhysicalPlan, LogicalPlan> toLogicalPlan = new HashMap<>();

        static ActionListener<Result> resultCloser = ActionListener.wrap(result -> result.pages().getFirst().close(), e -> {});

        TestRunner(long totalRows, long filteredRows) {
            this.totalRows = totalRows;
            this.filteredRows = filteredRows;
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
        public void run(PhysicalPlan physicalPlan, Configuration configuration, FoldContext foldContext, ActionListener<Result> listener) {
            LogicalPlan logicalPlan = toLogicalPlan.get(physicalPlan);
            invocations.add(logicalPlan);
            List<LogicalPlan> filters = logicalPlan.collect(plan -> plan instanceof Filter);
            long numResults = filters.isEmpty() ? totalRows : filteredRows;
            List<LogicalPlan> samples = logicalPlan.collect(plan -> plan instanceof Sample);
            for (LogicalPlan sample : samples) {
                numResults = (long) (numResults * (double) ((Literal) ((Sample) sample).probability()).value());
            }
            LongBlock block = blockFactory.newConstantLongBlockWith(numResults, 1);
            listener.onResponse(new Result(null, List.of(new Page(block)), null, null));
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
        assertError("SHOW INFO | STATS COUNT()", equalTo("line 1:1: query with [SHOWINFO] cannot be approximated"));
        assertError("TS test | STATS COUNT(emp_no)", equalTo("line 1:11: query with [TIMESERIESAGGREGATE] cannot be approximated"));
    }

    public void testVerify_incompatibleProcessingCommand() {
        assertError(
            "FROM test | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: query with [FORK] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:29: query with [FORK] cannot be approximated")
        );
        assertError(
            "FROM test | INLINE STATS COUNT() | STATS COUNT()",
            equalTo("line 1:13: query with [INLINESTATS] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | INLINE STATS COUNT()",
            equalTo("line 1:29: query with [INLINESTATS] cannot be approximated")
        );
        assertError(
            "FROM test | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: query with [LOOKUPJOIN] cannot be approximated")
        );
        assertError(
            "FROM test | STATS emp_no=COUNT() | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:36: query with [LOOKUPJOIN] cannot be approximated")
        );
    }

    public void testVerify_incompatibleAggregation() {
        assertError(
            "FROM test | SORT emp_no | STATS MIN(emp_no) | LIMIT 100",
            equalTo("line 1:33: aggregation function [MIN] cannot be approximated")
        );
        assertError(
            "FROM test | STATS SUM(emp_no), VALUES(emp_no), TOP(emp_no, 2, \"ASC\"), COUNT()",
            equalTo("line 1:32: aggregation function [VALUES] cannot be approximated")
        );
        assertError(
            "FROM test | STATS 5+10*POW(MAX(emp_no), 2) BY gender",
            equalTo("line 1:28: aggregation function [MAX] cannot be approximated")
        );
    }

    public void testCountPlan_largeDataNoFilters() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | STATS SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and approximation is executed immediately
        // after that with the correct sample probability.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(not(hasFilter("emp_no")), hasSample(1e-4)));
    }

    public void testCountPlan_smallDataNoFilters() throws Exception {
        TestRunner runner = new TestRunner(1_000, 1_000);
        Approximate approximate = createApproximate("FROM test | STATS SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and the original query is executed
        // immediately after that without sampling.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(not(hasFilter("emp_no")), not(hasSample())));
    }

    public void testCountPlan_largeDataAfterFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000_000L, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, then a few passes to get a good sample
        // probability, and finally approximation is executed.
        assertThat(runner.invocations, hasSize(4));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("emp_no"), hasSample(1e-7)));
        assertThat(runner.invocations.get(2), allOf(hasFilter("emp_no"), hasSample(1e-4)));
        assertThat(runner.invocations.get(3), allOf(hasFilter("emp_no"), hasSample(1e-4)));
    }

    public void testCountPlan_smallDataAfterFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000_000_000_000L, 100);
        Approximate approximate = createApproximate("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, then a few passes to get a good sample
        // probability, and finally the original query is executed without sampling.
        assertThat(runner.invocations, hasSize(5));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("emp_no"), hasSample(1e-13)));
        assertThat(runner.invocations.get(2), allOf(hasFilter("emp_no"), hasSample(1e-8)));
        assertThat(runner.invocations.get(3), allOf(hasFilter("emp_no"), hasSample(1e-3)));
        assertThat(runner.invocations.get(4), allOf(hasFilter("emp_no"), not(hasSample())));
    }

    public void testCountPlan_smallDataBeforeFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000, 10);
        Approximate approximate = createApproximate("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and the original query is executed
        // immediately after that without sampling.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter("emp_no")), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter("emp_no"), not(hasSample())));
    }

    public void testApproximate_countAllRows() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | STATS COUNT(*)", runner);
        approximate.approximate(TestRunner.resultCloser);
        assertThat(runner.invocations, hasSize(1));
    }

    public void testApproximate_countAllRows_withFiltering() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | WHERE emp_no > 10 | STATS COUNT(*)", runner);
        approximate.approximate(TestRunner.resultCloser);
        assertThat(runner.invocations, hasSize(greaterThan(1)));
    }

    public void testApproximate_countAllRows_withGrouping() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | STATS COUNT(*) BY emp_no", runner);
        approximate.approximate(TestRunner.resultCloser);
        assertThat(runner.invocations, hasSize(greaterThan(1)));
    }

    public void testApproximatePlan_createsConfidenceInterval_withoutGrouping() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | STATS COUNT(), SUM(emp_no)", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and approximation is executed immediately
        // after that with the correct sample probability.
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.get(1);
        assertThat(approximatePlan, hasSample(1e-4));
        // Counting all rows is exact, so no confidence interval is output.
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(COUNT())")));
        assertThat(approximatePlan, not(hasEval("RELIABLE(COUNT())")));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(SUM(emp_no))"));
        assertThat(approximatePlan, hasEval("RELIABLE(SUM(emp_no))"));
    }

    public void testApproximatePlan_createsConfidenceInterval_withGrouping() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate("FROM test | STATS COUNT(), SUM(emp_no) BY emp_no", runner);
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and approximation is executed immediately
        // after that with the correct sample probability.
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.get(1);
        assertThat(approximatePlan, hasSample(1e-4));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(COUNT())"));
        assertThat(approximatePlan, hasEval("RELIABLE(COUNT())"));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(SUM(emp_no))"));
        assertThat(approximatePlan, hasEval("RELIABLE(SUM(emp_no))"));
    }

    public void testApproximatePlan_dependentConfidenceIntervals() throws Exception {
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        Approximate approximate = createApproximate(
            "FROM test | STATS x=SUM(emp_no) | EVAL a=x*x, b=7, c=TO_STRING(x), d=MV_APPEND(x, 1::LONG), e=a+POW(b, 2)",
            runner
        );
        approximate.approximate(TestRunner.resultCloser);
        // One pass is needed to get the number of rows, and approximation is executed immediately
        // after that with the correct sample probability.
        assertThat(runner.invocations, hasSize(2));

        LogicalPlan approximatePlan = runner.invocations.get(1);
        assertThat(approximatePlan, hasPlan(Sample.class, s -> Foldables.literalValueOf(s.probability()).equals(1e-4)));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(x)"));
        assertThat(approximatePlan, hasEval("RELIABLE(x)"));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(a)"));
        assertThat(approximatePlan, hasEval("RELIABLE(a)"));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(b)")));
        assertThat(approximatePlan, not(hasEval("RELIABLE(b)")));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(c)")));
        assertThat(approximatePlan, not(hasEval("RELIABLE(c)")));
        assertThat(approximatePlan, not(hasEval("CONFIDENCE_INTERVAL(d)")));
        assertThat(approximatePlan, not(hasEval("RELIABLE(d)")));
        assertThat(approximatePlan, hasEval("CONFIDENCE_INTERVAL(e)"));
        assertThat(approximatePlan, hasEval("RELIABLE(e)"));
    }

    private Matcher<? super LogicalPlan> hasFilter(String field) {
        return hasPlan(
            Filter.class,
            filter -> filter.condition().anyMatch(expr -> expr instanceof NamedExpression ne && ne.name().equals(field))
        );
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

    private void verify(String query) throws Exception {
        Approximate.verifyPlan(getLogicalPlan(query));
    }

    private Approximate createApproximate(String query, TestRunner runner) throws Exception {
        return new Approximate(
            getLogicalPlan(query),
            new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), EsqlTestUtils.randomMinimumVersion())),
            runner,
            runner,
            EsqlTestUtils.TEST_CFG,
            FoldContext.small()
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

    public void test() {
        double bestEstimate = 17600.0;
        double[] estimates = new double[] {
            NaN, NaN, NaN, 93768.0, NaN, NaN, NaN, 93916.0, NaN, NaN, NaN, NaN, NaN, NaN, 93916.0, NaN,
            93916.0, NaN, NaN, NaN, NaN, NaN, 93768.0, NaN, NaN, NaN, NaN, NaN, NaN, 93916.0, NaN, NaN,
            93916.0, NaN, NaN, NaN, NaN, NaN, NaN, NaN, 93768.0, 93916.0, NaN, NaN, NaN, NaN, NaN, NaN
        };
        int trialCount=3;
        int bucketCount=16;
        System.out.println(Arrays.toString(ConfidenceInterval.computeConfidenceInterval(bestEstimate, estimates, trialCount, bucketCount, 0.9)));
    }
}
