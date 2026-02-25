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
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.session.Result;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

public class ApproximationBaseTestCase extends ESTestCase {

    private static final EsqlParser parser = EsqlParser.INSTANCE;
    private static final LogicalPlanPreOptimizer preOptimizer = new LogicalPlanPreOptimizer(
        new LogicalPreOptimizerContext(FoldContext.small(), mock(InferenceService.class), TransportVersion.current())
    );
    private static final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
    private static final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
    private static final MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

    static LogicalPlan getLogicalPlan(String query) throws Exception {
        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        LogicalPlan plan = parser.createStatement(query, new QueryParams()).plan();
        plan = AnalyzerTestUtils.defaultAnalyzer().analyze(plan);
        plan.setAnalyzed();
        preOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        return resultHolder.get().children().getFirst();
    }

    static Approximation.QueryProperties verify(String query) throws Exception {
        return Approximation.verifyPlan(getLogicalPlan(query));
    }

    static void assertError(String esql, Matcher<String> matcher) {
        Exception e = assertThrows(VerificationException.class, () -> verify(esql));
        assertThat(e.getMessage().substring("Found 1 problem\n".length()), matcher);
    }

    static Result newCountResult(long count) {
        LongBlock block = blockFactory.newConstantLongBlockWith(count, 1);
        return new Result(null, List.of(new Page(block)), null, null, null);
    }

    static Predicate<? super Aggregate> withAggs(Class<?>... aggs) {
        return aggregate -> aggregate.aggregates()
            .stream()
            .filter(agg -> agg instanceof Alias)
            .map(agg -> (Alias) agg)
            .map(Alias::child)
            .map(Object::getClass)
            .collect(Collectors.toSet())
            .equals(Set.of(aggs));
    }

    static Predicate<SampledAggregate> withProbability(double probablity) {
        return sampledAggregate -> (double) Foldables.literalValueOf(sampledAggregate.sampleProbability()) == probablity;
    }

    static Predicate<Eval> withField(String field) {
        return eval -> eval.fields().stream().anyMatch(alias -> alias.name().equals(field));
    }

    @SafeVarargs
    static <E extends LogicalPlan> Matcher<? super LogicalPlan> hasPlan(Class<E> typeToken, Predicate<? super E>... predicates) {
        return new TypeSafeMatcher<>() {
            @Override
            @SuppressWarnings("unchecked")
            protected boolean matchesSafely(LogicalPlan logicalPlan) {
                return logicalPlan.anyMatch(
                    plan -> plan.getClass() == typeToken && Arrays.stream(predicates).allMatch(predicate -> predicate.test((E) plan))
                );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a plan containing [" + typeToken.getSimpleName() + "] matching the predicate");
            }
        };
    }
}
