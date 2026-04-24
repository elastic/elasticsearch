/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestOptimizer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.expression.Foldables;
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

import static org.elasticsearch.xpack.esql.EsqlTestUtils.optimizer;

public abstract class ApproximationTestCase extends ESTestCase {

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final TestOptimizer optimizer = optimizer().addDefaultIndex().addTestLookup().addK8s();

    static LogicalPlan getLogicalPlan(String query) {
        return optimizer.coordinatorPlan(query);
    }

    static Approximation.QueryProperties verify(String query) {
        return Approximation.verifyPlanOrThrow(getLogicalPlan(query));
    }

    static void assertError(String esql, Matcher<String> matcher) {
        Exception e = assertThrows(VerificationException.class, () -> verify(esql));
        assertThat(e.getMessage(), matcher);
    }

    static Result newCountResult(long count) {
        LongBlock block = blockFactory.newConstantLongBlockWith(count, 1);
        return new Result(null, List.of(new Page(block)), null, null, null, null);
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
