/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;
import java.util.Locale;
import java.util.Set;

public class Approximate {

    public interface LogicalPlanRunner {
        void run(LogicalPlan plan, ActionListener<Result> listener);
    }

    private static final Set<Class<? extends LogicalPlan>> SWAPPABLE_WITH_SAMPLE = Set.of(
        Dissect.class,
        Drop.class,
        Eval.class,
        Filter.class,
        Grok.class,
        Keep.class,
        OrderBy.class,
        Rename.class,
        Sample.class
    );

    // TODO: not a good value
    private static final int SAMPLE_ROW_COUNT = 1000;

    private final LogicalPlan logicalPlan;

    public Approximate(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
        verifyPlan();
    }

    /**
     * Computes approximate results for the given logical plan.
     *
     * This works by first executing a plan that counts the number of rows
     * getting to the aggregation. That count is used to compute a sample
     * probability, which is then used to sample approximately 1000 rows
     * to aggregate over and approximate the aggregation.
     */
    public void approximate(LogicalPlanRunner runner, ActionListener<Result> listener) {
        runner.run(
            countPlan(),
            listener.delegateFailureAndWrap(
                (countListener, countResult) -> runner.run(approximatePlan(sampleProbability(countResult)), listener)
            )
        );
    }

    /**
     * Verifies that a plan is suitable for approximation.
     *
     * To be so, the plan must contain at least one STATS function, and all
     * functions between the source and the leftmost STATS function must be
     * swappable with SAMPLE.
     *
     * In that case, the STATS can be replaced by SAMPLE, STATS with sample
     * correction terms, and the SAMPLE can be moved to the source and
     * executed inside Lucene.
     */
    private void verifyPlan() {
        if (logicalPlan.preOptimized() == false) {
            throw new IllegalStateException("Expected pre-optimized plan");
        }

        if (logicalPlan.anyMatch(plan -> plan instanceof Aggregate) == false) {
            throw new InvalidArgumentException("query without [STATS] function cannot be approximated");
        }

        Holder<Boolean> encounteredStats = new Holder<>(false);
        logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                encounteredStats.set(false);
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate) {
                    encounteredStats.set(true);
                } else if (SWAPPABLE_WITH_SAMPLE.contains(plan.getClass()) == false) {
                    throw new InvalidArgumentException(
                        "query with [" + plan.nodeName().toUpperCase(Locale.ROOT) + "] before [STATS] function cannot be approximated"
                    );
                }
            }
            return plan;
        });
    }

    /**
     * Returns a plan that counts the number of rows of the original plan that
     * would reach the leftmost STATS function. So it's the original plan cut
     * off at the leftmost STATS function, followed by "| STATS COUNT(*)".
     * This value can be used to pick a good sample probability.
     */
    private LogicalPlan countPlan() {
        Holder<Boolean> encounteredStats = new Holder<>(false);
        LogicalPlan countPlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                encounteredStats.set(false);
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    encounteredStats.set(true);
                    plan = new Aggregate(
                        Source.EMPTY,
                        aggregate.child(),
                        List.of(),
                        List.of(new Alias(Source.EMPTY, "approximate-count", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*"))))
                    );
                }
            } else {
                plan = plan.children().getFirst();
            }
            return plan;
        });

        countPlan.setPreOptimized();
        return countPlan;
    }

    /**
     * Returns a sample probability based on the total number of rows.
     */
    private double sampleProbability(Result countResult) {
        long rowCount = ((LongBlock) (countResult.pages().getFirst().getBlock(0))).getLong(0);
        return rowCount <= SAMPLE_ROW_COUNT ? 1.0 : (double) SAMPLE_ROW_COUNT / rowCount;
    }

    /**
     * Returns a plan that approximates the original plan. It consists of the
     * original plan, with the leftmost STATS function replaced by:
     * "SAMPLE probability | STATS sample_corrected_aggs".
     */
    private LogicalPlan approximatePlan(double sampleProbability) {
        Holder<Boolean> encounteredStats = new Holder<>(false);
        LogicalPlan approximatePlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                encounteredStats.set(false);
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    encounteredStats.set(true);
                    Expression sampleProbabilityExpr = new Literal(Source.EMPTY, sampleProbability, DataType.DOUBLE);
                    Sample sample = new Sample(Source.EMPTY, sampleProbabilityExpr, aggregate.child());
                    plan = aggregate.replaceChild(sample);
                    plan = plan.transformExpressionsOnlyUp(
                        expr -> expr instanceof NeedsSampleCorrection nsc ? nsc.sampleCorrection(sampleProbabilityExpr) : expr
                    );
                }
            }
            return plan;
        });

        approximatePlan.setPreOptimized();
        return approximatePlan;
    }
}
