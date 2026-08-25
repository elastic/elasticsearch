/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug tests")
public abstract class AbstractPromqlPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * PromQL is translated into non-finite-preserving arithmetic, which the optimizer downgrades to the finite-only
     * variant on clusters predating {@link NonFiniteSupport#ESQL_PROMQL_NON_FINITE_MATH}. The shared optimizer draws an
     * arbitrary transport version, which would silently strip those semantics, so PromQL plans are optimized against a
     * version that supports them. Downgrade behavior itself is covered by
     * {@code SubstituteTransportVersionAwareExpressionsTests}.
     */
    private static LogicalPlanOptimizer promqlOptimizer;

    @BeforeClass
    public static void initPromqlOptimizer() {
        promqlOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext(NonFiniteSupport.ESQL_PROMQL_NON_FINITE_MATH));
    }

    protected static TestAnalyzer tsAnalyzer() {
        return analyzerWithEnrichPolicies().addK8s()
            .addK8sDateNanos()
            .addOtelMetrics()
            .addEmptyIndex()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .minimumTransportVersion(TimeSeriesCollapse.TS_COLLAPSE);
    }

    protected LogicalPlan planPromql(String query) {
        return planPromql(query, false, true);
    }

    protected LogicalPlan planPromql(String query, boolean optimize) {
        return planPromql(query, false, optimize);
    }

    protected LogicalPlan planPromql(String query, boolean allowEmptyReferences, boolean optimize) {
        var now = Instant.now();
        query = query.replace("$now-1h", "\"" + now.minus(1, ChronoUnit.HOURS) + "\"");
        query = query.replace("$now", "\"" + now + "\"");
        var analyzed = tsAnalyzer().query(query);
        AttributeSet.Builder references = AttributeSet.builder();
        analyzed.forEachDown(lp -> references.addAll(lp.references()));
        if (allowEmptyReferences) {
            assertThat(references.build(), empty());
        } else {
            assertThat(references.build(), not(empty()));
        }
        logger.trace("analyzed plan:\n{}", analyzed);
        if (optimize == false) {
            return analyzed;
        }
        var optimized = promqlOptimizer.optimize(analyzed);
        logger.trace("optimized plan:\n{}", optimized);
        return optimized;
    }

    protected void assertConstantResult(String query, Matcher<Double> matcher) {
        var plan = planPromql("PROMQL index=k8s step=1m " + query, true);
        Eval eval = plan.collect(Eval.class).getFirst();
        Literal literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(as(literal.value(), Double.class), matcher);

        Aggregate aggregate = eval.collect(Aggregate.class).getFirst();
        ReferenceAttribute step = as(aggregate.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(step.name(), equalTo("step"));

        TimeSeriesAggregate tsAgg = aggregate.collect(TimeSeriesAggregate.class).getFirst();
        ReferenceAttribute stepInTsAgg = as(Alias.unwrap(tsAgg.aggregates().getFirst()), ReferenceAttribute.class);
        assertThat(stepInTsAgg.name(), equalTo("step"));

        Eval stepEval = tsAgg.collect(Eval.class).getFirst();
        Alias bucketAlias = as(stepEval.fields().getFirst(), Alias.class);
        assertThat(bucketAlias.id(), equalTo(stepInTsAgg.id()));
        assertThat(bucketAlias.id(), equalTo(step.id()));
    }
}
