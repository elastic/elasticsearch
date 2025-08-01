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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.session.Result;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class ApproximateTests extends ESTestCase {

    private static final EsqlParser parser = EsqlParser.INSTANCE;
    private static final LogicalPlanPreOptimizer logicalPlanPreOptimizer = new LogicalPlanPreOptimizer(
        new LogicalPreOptimizerContext(FoldContext.small(), mock(InferenceService.class), TransportVersion.current())
    );
    private static final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
    private static final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
    private static final MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

    public void testVerify_validQuery() throws Exception {
        createApproximate("FROM index | EVAL x=1 | WHERE y<0.1 | SORT z | SAMPLE 0.1 | STATS COUNT() BY y | SORT z | MV_EXPAND x");
    }

    public void testVerify_noStats() {
        assertError("FROM index | EVAL x = 1 | SORT y", equalTo("line 1:1: query without [STATS] cannot be approximated"));
    }

    public void testVerify_incompatibleCommand() {
        assertError(
            "FROM index | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:14: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:30: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );

        assertError(
            "FROM index | INLINE STATS COUNT() | STATS COUNT()",
            equalTo("line 1:14: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | INLINE STATS COUNT()",
            equalTo("line 1:30: query with [INLINE STATS COUNT()] cannot be approximated")
        );

        assertError(
            "FROM index | LOOKUP JOIN index2 ON id | STATS COUNT()",
            equalTo("line 1:14: query with [LOOKUP JOIN index2 ON id] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | LOOKUP JOIN index2 ON id",
            equalTo("line 1:30: query with [LOOKUP JOIN index2 ON id] cannot be approximated")
        );
    }

    public void testVerify_nonSwappableCommand() {
        assertError(
            "FROM index | MV_EXPAND x | STATS COUNT()",
            equalTo("line 1:14: query with [MV_EXPAND x] before [STATS] cannot be approximated")
        );
        assertError(
            "FROM index | CHANGE_POINT x ON t | STATS COUNT()",
            equalTo("line 1:14: query with [CHANGE_POINT x ON t] before [STATS] cannot be approximated")
        );
        assertError(
            "FROM index | LIMIT 100 | STATS COUNT()",
            equalTo("line 1:14: query with [LIMIT 100] before [STATS] cannot be approximated")
        );
    }

    /**
     * Runner that simulates the execution of an ESQL query.
     *
     * The runner always returns a result with one field: the number of rows.
     *
     * The runner is initialized with a total number of rows (returned when
     * there are no filters in the query), and a number of filtered rows
     * (returned when there are filters in the query). If there's random
     * sampling in the query, the returned number of rows is multiplied by
     * the sampling probability.
     *
     * The runner collects all its invocations.
     */
    private static class TestRunner implements Approximate.LogicalPlanRunner {

        private final long totalRows;
        private final long filteredRows;
        private final List<LogicalPlan> invocations;

        TestRunner(long totalRows, long filteredRows) {
            this.totalRows = totalRows;
            this.filteredRows = filteredRows;
            this.invocations = new ArrayList<>();
        }

        @Override
        public void run(LogicalPlan logicalPlan, ActionListener<Result> listener) {
            invocations.add(logicalPlan);
            List<LogicalPlan> filter = logicalPlan.collect(plan -> plan instanceof Filter);
            long numResults = filter.isEmpty() ? totalRows : filteredRows;
            List<LogicalPlan> sample = logicalPlan.collect(plan -> plan instanceof Sample);
            if (sample.isEmpty() == false) {
                numResults = (long) (numResults * (double) ((Literal) ((Sample) sample.getFirst()).probability()).value());
            }
            LongBlock block = blockFactory.newConstantLongBlockWith(numResults, 1);
            listener.onResponse(new Result(null, List.of(new Page(block)), null, null));
        }
    }

    public void testApproximate_largeDataNoFilters() throws Exception {
        Approximate approximate = createApproximate("FROM index | STATS SUM(x), AVG(x)");
        TestRunner runner = new TestRunner(1_000_000_000, 1_000_000_000);
        approximate.approximate(runner, ActionListener.noop());
        // One pass is needed to get the number of rows, and approximation is executed immediately
        // after that with the correct sample probability.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter()), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(not(hasFilter()), hasSample(1e-5)));
    }

    public void testApproximate_smallDataNoFilters() throws Exception {
        Approximate approximate = createApproximate("FROM index | STATS SUM(x), AVG(x)");
        TestRunner runner = new TestRunner(1_000, 1_000);
        approximate.approximate(runner, ActionListener.noop());
        // One pass is needed to get the number of rows, and the original query is executed
        // immediately after that without sampling.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter()), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(not(hasFilter()), not(hasSample())));
    }

    public void testApproximate_largeDataAfterFiltering() throws Exception {
        Approximate approximate = createApproximate("FROM index | WHERE t < 1 | STATS SUM(x), AVG(x)");
        TestRunner runner = new TestRunner(1_000_000_000_000L, 1_000_000_000);
        approximate.approximate(runner, ActionListener.noop());
        // One pass is needed to get the number of rows, then a few passes to get a good sample
        // probability, and finally approximation is executed.
        assertThat(runner.invocations, hasSize(4));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter()), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter(), hasSample(1e-8)));
        assertThat(runner.invocations.get(2), allOf(hasFilter(), hasSample(1e-5)));
        assertThat(runner.invocations.get(3), allOf(hasFilter(), hasSample(1e-5)));
    }

    public void testApproximate_smallDataAfterFiltering() throws Exception {
        Approximate approximate = createApproximate("FROM index | WHERE t < 1 | STATS SUM(x), AVG(x)");
        TestRunner runner = new TestRunner(1_000_000_000_000_000_000L, 100);
        approximate.approximate(runner, ActionListener.noop());
        // One pass is needed to get the number of rows, then a few passes to get a good sample
        // probability, and finally the original query is executed without sampling.
        assertThat(runner.invocations, hasSize(6));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter()), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter(), hasSample(1e-14)));
        assertThat(runner.invocations.get(2), allOf(hasFilter(), hasSample(1e-10)));
        assertThat(runner.invocations.get(3), allOf(hasFilter(), hasSample(1e-6)));
        assertThat(runner.invocations.get(4), allOf(hasFilter(), hasSample(1e-2)));
        assertThat(runner.invocations.get(5), allOf(hasFilter(), not(hasSample())));
    }

    public void testApproximate_smallDataBeforeFiltering() throws Exception {
        Approximate approximate = createApproximate("FROM index | WHERE t < 1 | STATS SUM(x), AVG(x)");
        TestRunner runner = new TestRunner(1_000, 10);
        approximate.approximate(runner, ActionListener.noop());
        // One pass is needed to get the number of rows, and the original query is executed
        // immediately after that without sampling.
        assertThat(runner.invocations, hasSize(2));
        assertThat(runner.invocations.get(0), allOf(not(hasFilter()), not(hasSample())));
        assertThat(runner.invocations.get(1), allOf(hasFilter(), not(hasSample())));
    }

    private Matcher<? super LogicalPlan> hasFilter() {
        return new TypeSafeMatcher<>() {

            @Override
            protected boolean matchesSafely(LogicalPlan logicalPlan) {
                return logicalPlan.anyMatch(plan -> plan instanceof Filter);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a plan containing a Filter");
            }
        };
    }

    private Matcher<? super LogicalPlan> hasSample() {
        return hasSample(null);
    }

    private Matcher<? super LogicalPlan> hasSample(Double probability) {
        return new TypeSafeMatcher<>() {

            @Override
            protected boolean matchesSafely(LogicalPlan logicalPlan) {
                return logicalPlan.anyMatch(
                    plan -> plan instanceof Sample
                        && (probability == null || ((Literal) ((Sample) plan).probability()).value().equals(probability))
                );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a plan containing a Sample");
                if (probability != null) {
                    description.appendText(" with probability " + probability);
                }
            }
        };
    }

    private void assertError(String esql, Matcher<String> matcher) {
        Exception e = assertThrows(VerificationException.class, () -> createApproximate(esql));
        assertThat(e.getMessage().substring("Found 1 problem\n".length()), matcher);
    }

    private Approximate createApproximate(String esql) throws Exception {
        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        LogicalPlan plan = parser.createStatement(esql, new QueryParams()).plan();
        plan.setAnalyzed();
        logicalPlanPreOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        return new Approximate(resultHolder.get());
    }
}
