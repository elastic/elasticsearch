/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.ConfidenceInterval;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAppend;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class computes approximate results for certain classes of ES|QL queries.
 * Approximate results are usually much faster to compute than exact results.
 * <p>
 * A query is currently suitable for approximation if:
 * <ul>
 *   <li> it contains exactly one {@code STATS} command
 *   <li> the other processing commands are from the supported set
 *        ({@link Approximate#SUPPORTED_COMMANDS}); this set contains almost all
 *        unary commands, but most notably not {@code FORK} or {@code JOIN}.
 *   <li> the aggregate functions are from the supported set
 *        ({@link Approximate#SUPPORTED_SINGLE_VALUED_AGGS} and
 *         {@link Approximate#SUPPORTED_MULTIVALUED_AGGS})
 * </ul>
 * Some of these restrictions may be lifted in the future.
 * <p>
 *
 * When these conditions are met, the query is replaced by an approximation query
 * that samples documents before the STATS, and extrapolates the aggregate
 * functions if needed. This new logical plan is generated by
 * {@link Approximate#approximatePlan}.
 * <p>
 * In addition to approximate results, confidence intervals are also computed.
 * This is done by dividing the sampled rows {@link Approximate#TRIAL_COUNT}
 * times into {@link Approximate#BUCKET_COUNT} buckets, computing the aggregate
 * functions for each bucket, and using these sampled values to compute
 * confidence intervals.
 * <p>
 * To obtain an appropriate sample probability, first a target number of rows
 * is set. For now this is a fixed number ({@link Approximate#SAMPLE_ROW_COUNT}).
 * Next, the total number of rows in the source index is counted via the plan
 * {@link Approximate#sourceCountPlan}. This plan should execute fast. When
 * there are no commands that can change the number of rows, the sample
 * probability can be directly computed as a ratio of the target number of rows
 * and this total number.
 * <p>
 * In the presence of commands that can change to number of rows, another step
 * is needed. The initial sample probability is set to the ratio above and the
 * number of rows is sampled with the plan {@link Approximate#countPlan}. As
 * long as the sampled number of rows is smaller than intended, the probability
 * is scaled up until a good probability is reached. This final probability is
 * used for approximating the original plan.
 */
public class Approximate {

    public record QueryProperties(boolean canDecreaseRowCount, boolean canIncreaseRowCount) {}

    /**
     * These processing commands are supported.
     */
    private static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS = Set.of(
        Aggregate.class,
        ChangePoint.class,
        Completion.class,
        Dissect.class,
        Drop.class,
        Enrich.class,
        EsqlProject.class,
        EsRelation.class,
        Eval.class,
        Filter.class,
        Grok.class,
        Insist.class,
        Keep.class,
        Limit.class,
        MvExpand.class,
        OrderBy.class,
        Project.class,
        RegexExtract.class,
        Rename.class,
        Rerank.class,
        Row.class,
        Sample.class,
        TopN.class
    );

    /**
     * These commands preserve all rows, making it easy to predict the number of output rows.
     */
    private static final Set<Class<? extends LogicalPlan>> ROW_PRESERVING_COMMANDS = Set.of(
        ChangePoint.class,
        Completion.class,
        Dissect.class,
        Drop.class,
        Enrich.class,
        EsqlProject.class,
        Eval.class,
        Grok.class,
        Insist.class,
        Keep.class,
        OrderBy.class,
        Project.class,
        RegexExtract.class,
        Rename.class,
        Rerank.class
    );

    /**
     * These commands never increase the number of all rows, making it easier to predict the number of output rows.
     */
    private static final Set<Class<? extends LogicalPlan>> ROW_NON_INCREASING_COMMANDS = Sets.union(
        Set.of(Filter.class, Limit.class, Sample.class, TopN.class),
        ROW_PRESERVING_COMMANDS
    );

    /**
     * These commands never decrease the number of all rows, making it easier to predict the number of output rows.
     */
    private static final Set<Class<? extends LogicalPlan>> ROW_NON_DECREASING_COMMANDS = Sets.union(
        Set.of(MvExpand.class),
        ROW_PRESERVING_COMMANDS
    );

    /**
     * These aggregate functions behave well with random sampling, in the sense
     * that they converge to the true value as the sample size increases.
     */
    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_SINGLE_VALUED_AGGS = Set.of(
        Avg.class,
        Count.class,
        Median.class,
        MedianAbsoluteDeviation.class,
        Percentile.class,
        StdDev.class,
        Sum.class,
        WeightedAvg.class
    );

    /**
     * These aggregate functions need to be corrected for random sampling, by
     * scaling up the sampled value by the inverse of the sampling probability.
     * Other aggregate functions do not need any correction.
     */
    private static final Set<Class<? extends AggregateFunction>> SAMPLE_CORRECTED_AGGS = Set.of(Count.class, Sum.class);

    /**
     * These multivalued aggregate functions work well with random sampling.
     * However, confidence intervals make no sense anymore and are dropped.
     */
    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_MULTIVALUED_AGGS = Set.of(
        org.elasticsearch.xpack.esql.expression.function.aggregate.Sample.class
    );

    /**
     * These scalar functions produce multivalued output.
     */
    private static final Set<Class<? extends EsqlScalarFunction>> MULTIVALUED_OUTPUT_FUNCTIONS = Set.of(MvAppend.class);

    /**
     * The target number of sampled rows to estimate the total number of rows
     * reaching the STATS function. This is only relevant when there are commands
     * that can change the number of rows. This value leads to an error of about
     * 1% in the estimated count.
     */
    private static final int SAMPLE_ROW_COUNT_FOR_COUNT_ESTIMATION = 10_000;

    // TODO: set via a query setting; and find a good default value
    private static final int SAMPLE_ROW_COUNT = 100_000;

    // TODO: set via a query setting
    private static final double CONFIDENCE_LEVEL = 0.90;

    /**
     * Don't sample with a probability higher than this threshold. The cost of
     * tracking confidence intervals doesn't outweigh the benefits of sampling.
     */
    private static final double SAMPLE_PROBABILITY_THRESHOLD = 0.1;

    /**
     * The number of times (trials) the sampled rows are divided into buckets.
     */
    private static final int TRIAL_COUNT = 2;

    /**
     * The number of buckets to use for computing confidence intervals.
     */
    private static final int BUCKET_COUNT = 16;

    private static final Logger logger = LogManager.getLogger(Approximate.class);

    private static final AggregateFunction COUNT_ALL_ROWS = new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, StringUtils.WILDCARD));

    private final LogicalPlan logicalPlan;
    private final QueryProperties queryProperties;
    private final EsqlSession.PlanRunner runner;
    private final LogicalPlanOptimizer logicalPlanOptimizer;
    private final Function<LogicalPlan, PhysicalPlan> toPhysicalPlan;
    private final Configuration configuration;
    private final FoldContext foldContext;

    private long sourceRowCount;

    public Approximate(
        LogicalPlan logicalPlan,
        LogicalPlanOptimizer logicalPlanOptimizer,
        Function<LogicalPlan, PhysicalPlan> toPhysicalPlan,
        EsqlSession.PlanRunner runner,
        Configuration configuration,
        FoldContext foldContext
    ) {
        this.logicalPlan = logicalPlan;
        this.queryProperties = verifyPlan(logicalPlan);
        this.logicalPlanOptimizer = logicalPlanOptimizer;
        this.toPhysicalPlan = toPhysicalPlan;
        this.runner = runner;
        this.configuration = configuration;
        this.foldContext = foldContext;
    }

    /**
     * Verifies that a plan is suitable for approximation.
     *
     * @return whether the part of the query until the STATS command preserves all rows
     * @throws VerificationException if the plan is not suitable for approximation
     */
    public static QueryProperties verifyPlan(LogicalPlan logicalPlan) throws VerificationException {
        if (logicalPlan.preOptimized() == false) {
            throw new IllegalStateException("Expected pre-optimized plan");
        }
        // The plan must contain a STATS command.
        if (logicalPlan.anyMatch(plan -> plan instanceof Aggregate) == false) {
            throw new VerificationException(
                List.of(Failure.fail(logicalPlan.collectLeaves().getFirst(), "query without [STATS] cannot be approximated"))
            );
        }
        // Verify that all commands are supported.
        logicalPlan.forEachUp(plan -> {
            if (SUPPORTED_COMMANDS.contains(plan.getClass()) == false) {
                // TODO: better way of obtaining the command name
                throw new VerificationException(
                    List.of(Failure.fail(plan, "query with [" + plan.nodeName().toUpperCase(Locale.ROOT) + "] cannot be approximated"))
                );
            }
        });

        Holder<Boolean> encounteredStats = new Holder<>(false);
        Holder<Boolean> canIncreaseRowCount = new Holder<>(false);
        Holder<Boolean> canDecreaseRowCount = new Holder<>(false);

        logicalPlan.transformUp(plan -> {
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    // Verify that the aggregate functions are supported.
                    encounteredStats.set(true);
                    plan.transformExpressionsOnly(AggregateFunction.class, aggFn -> {
                        if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass()) == false
                            && SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
                            // TODO: better way of obtaining the agg name
                            throw new VerificationException(
                                List.of(
                                    Failure.fail(
                                        aggFn,
                                        "aggregation function [" + aggFn.nodeName().toUpperCase(Locale.ROOT) + "] cannot be approximated"
                                    )
                                )
                            );
                        }
                        return aggFn;
                    });
                } else if (plan instanceof LeafPlan == false) {
                    if (ROW_NON_DECREASING_COMMANDS.contains(plan.getClass()) == false) {
                        canDecreaseRowCount.set(true);
                    }
                    if (ROW_NON_INCREASING_COMMANDS.contains(plan.getClass()) == false) {
                        canIncreaseRowCount.set(true);
                    }
                }
            } else {
                // Multiple STATS commands are not supported.
                if (plan instanceof Aggregate) {
                    throw new VerificationException(List.of(Failure.fail(plan, "query with multiple [STATS] cannot be approximated")));
                }
            }
            return plan;
        });

        return new QueryProperties(canDecreaseRowCount.get(), canIncreaseRowCount.get());
    }

    /**
     * Computes approximate results for the logical plan.
     */
    public void approximate(ActionListener<Result> listener) {
        // Try to execute the query if it translates to an ES stats query. Results for
        // them come from Lucene's metadata and are computed fast. Approximation would
        // only slow things down in that case. When the query is not an ES stats query,
        // an exception is thrown and approximation is attempted.
        runner.run(
            toPhysicalPlan.apply(logicalPlan),
            configuration.throwOnNonEsStatsQuery(true),
            foldContext,
            approximateListener(listener)
        );
    }

    private ActionListener<Result> approximateListener(ActionListener<Result> listener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Result result) {
                boolean esStatsQueryExecuted = result.executionInfo() != null
                    && result.executionInfo().clusterInfo.values()
                        .stream()
                        .noneMatch(cluster -> cluster.getFailures().stream().anyMatch(Approximate::isCausedByUnsupported));
                if (esStatsQueryExecuted) {
                    logger.debug("stats query succeeded; returning exact result");
                    listener.onResponse(result);
                } else {
                    result.pages().forEach(Page::close);
                    runner.run(toPhysicalPlan.apply(sourceCountPlan()), configuration, foldContext, sourceCountListener(listener));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (isCausedByUnsupported(e)) {
                    runner.run(toPhysicalPlan.apply(sourceCountPlan()), configuration, foldContext, sourceCountListener(listener));
                } else {
                    logger.debug("stats query failed; returning error", e);
                    listener.onFailure(e);
                }
            }
        };
    }

    private static boolean isCausedByUnsupported(Exception ex) {
        return ExceptionsHelper.unwrapCausesAndSuppressed(ex, e -> e instanceof UnsupportedOperationException).isPresent();
    }

    /**
     * Plan that counts the number of rows in the source index.
     * This is the ES|QL query:
     * <pre>
     * {@code FROM index | STATS COUNT(*)}
     * </pre>
     */
    private LogicalPlan sourceCountPlan() {
        LogicalPlan sourceCountPlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                // Append the leaf plan by a STATS COUNT(*).
                plan = new Aggregate(Source.EMPTY, plan, List.of(), List.of(new Alias(Source.EMPTY, "$count", COUNT_ALL_ROWS)));
            } else {
                // Strip everything after the leaf.
                plan = plan.children().getFirst();
            }
            return plan;
        });

        sourceCountPlan.setPreOptimized();
        return logicalPlanOptimizer.optimize(sourceCountPlan);
    }

    /**
     * Receives the total number of rows, and runs either the
     * {@link Approximate#approximatePlan} or {@link Approximate#countPlan}
     * depending on whether the original query preserves all rows or not.
     */
    private ActionListener<Result> sourceCountListener(ActionListener<Result> listener) {
        return listener.delegateFailureAndWrap((countListener, countResult) -> {
            sourceRowCount = rowCount(countResult);
            logger.debug("sourceCountPlan result: {} rows", sourceRowCount);
            if (sourceRowCount == 0) {
                // If there are no rows, run the original query.
                resetExecutionInfo(countResult);
                runner.run(toPhysicalPlan.apply(logicalPlan), configuration, foldContext, listener);
                return;
            }
            double sampleProbability = Math.min(1.0, (double) SAMPLE_ROW_COUNT / sourceRowCount);
            if (queryProperties.canIncreaseRowCount == false && queryProperties.canDecreaseRowCount == false) {
                // If the query preserves all rows, we can directly approximate with the sample probability.
                resetExecutionInfo(countResult);
                runner.run(toPhysicalPlan.apply(approximatePlan(sampleProbability)), configuration, foldContext, listener);
            } else if (queryProperties.canIncreaseRowCount == false && sampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
                // If the query cannot increase the number of rows, and the sample probability is large,
                // we can directly run the original query without sampling.
                logger.debug("using original plan (too few rows)");
                resetExecutionInfo(countResult);
                runner.run(toPhysicalPlan.apply(logicalPlan), configuration, foldContext, listener);
            } else {
                // Otherwise, we need to sample the number of rows first to obtain a good sample probability.
                sampleProbability = Math.min(1.0, (double) SAMPLE_ROW_COUNT_FOR_COUNT_ESTIMATION / sourceRowCount);
                runner.run(
                    toPhysicalPlan.apply(countPlan(sampleProbability)),
                    configuration,
                    foldContext,
                    countListener(sampleProbability, listener)
                );
            }
        });
    }

    private void resetExecutionInfo(Result result) {
        if (result.executionInfo() != null) {
            result.executionInfo().reset();
        }
    }

    /**
     * Plan that counts a sampled number of rows reaching the STATS function.
     * This is the ES|QL query
     * <pre>
     * {@code FROM index | SAMPLE prob | (...) | STATS COUNT(*)}
     * </pre>
     * where {@code prob} is the given sample probability.
     */
    private LogicalPlan countPlan(double sampleProbability) {
        Holder<Boolean> encounteredStats = new Holder<>(false);
        LogicalPlan countPlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                if (sampleProbability < 1.0) {
                    // The leaf plan should be appended by a SAMPLE.
                    plan = new Sample(Source.EMPTY, Literal.fromDouble(Source.EMPTY, sampleProbability), plan);
                }
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    // The STATS function should be replaced by a STATS COUNT(*).
                    encounteredStats.set(true);
                    plan = new Aggregate(
                        Source.EMPTY,
                        aggregate.child(),
                        List.of(),
                        List.of(new Alias(Source.EMPTY, "$count", COUNT_ALL_ROWS))
                    );
                }
            } else {
                // Strip everything after the STATS command.
                plan = plan.children().getFirst();
            }
            return plan;
        });

        countPlan.setPreOptimized();
        return logicalPlanOptimizer.optimize(countPlan);
    }

    /**
     * Receives the sampled number of rows reaching the STATS function.
     * Runs either the {@link Approximate#approximatePlan} or a next iteration
     * {@link Approximate#countPlan} depending on whether the current count is
     * sufficient.
     */
    private ActionListener<Result> countListener(double sampleProbability, ActionListener<Result> listener) {
        return listener.delegateFailureAndWrap((countListener, countResult) -> {
            long rowCount = rowCount(countResult);
            logger.debug("countPlan result (p={}): {} rows", sampleProbability, rowCount);
            double newSampleProbability = Math.min(1.0, sampleProbability * SAMPLE_ROW_COUNT / Math.max(1, rowCount));
            if (newSampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
                // If the new sample probability is large, run the original query.
                logger.debug("using original plan (too few rows)");
                resetExecutionInfo(countResult);
                runner.run(toPhysicalPlan.apply(logicalPlan), configuration, foldContext, listener);
            } else if (rowCount <= SAMPLE_ROW_COUNT_FOR_COUNT_ESTIMATION / 2) {
                // Not enough rows are sampled yet; increase the sample probability and try again.
                newSampleProbability = Math.min(1.0, sampleProbability * SAMPLE_ROW_COUNT_FOR_COUNT_ESTIMATION / Math.max(1, rowCount));
                runner.run(
                    toPhysicalPlan.apply(countPlan(newSampleProbability)),
                    configuration,
                    foldContext,
                    countListener(newSampleProbability, listener)
                );
            } else {
                resetExecutionInfo(countResult);
                runner.run(toPhysicalPlan.apply(approximatePlan(newSampleProbability)), configuration, foldContext, listener);
            }
        });
    }

    /**
     * Returns the row count in the result and closes the result.
     */
    private long rowCount(Result countResult) {
        long rowCount = ((LongBlock) (countResult.pages().getFirst().getBlock(0))).getLong(0);
        countResult.pages().getFirst().close();
        return rowCount;
    }

    /**
     * Returns a plan that approximates the original plan and computes confidence intervals.
     * This approximation query consists of the following:
     * <ul>
     *     <li> Source command
     *     <li> {@code SAMPLE} with the provided sample probability
     *     <li> All commands before the {@code STATS} command
     *     <li> {@code EVAL} adding a new column with random bucket IDs for each trial
     *     <li> {@code STATS} command with:
     *          <ul>
     *              <li> Each aggregate function replaced by a sample-corrected version (if needed)
     *              <li> {@link Approximate#TRIAL_COUNT} * {@link Approximate#BUCKET_COUNT} additional columns
     *                   with a sampled values for each aggregate function, sample-corrected (if needed)
     *          </ul>
     *     <li> All commands after the {@code STATS} command, modified to also process
     *          the additional bucket columns where possible
     *     <li> {@code EVAL} to compute confidence intervals for all fields with buckets
     *     <li> {@code FILTER} to remove rows with null confidence intervals (due to very little data)
     *     <li> {@code PROJECT} to drop all bucket columns
     * </ul>
     *
     * As an example, the simple query:
     * <pre>
     *     {@code
     *         FROM index
     *             | EVAL x = 2*x
     *             | STATS s = SUM(x) BY group
     *             | EVAL s2 = s*s
     *     }
     * </pre>
     * is rewritten to (prob=sampleProbability, T=trialCount, B=bucketCount):
     * <pre>
     *     {@code
     *         FROM index
     *             | SAMPLE prob
     *             | EVAL x = 2*x
     *             | EVAL bucketId = MV_APPEND(RANDOM(B), ... , RANDOM(B))  // T times
     *             | STATS s = SUM(x) / prob,
     *                     `s$0` = SUM(x) / (prob/B)) WHERE MV_SLICE(bucketId, 0, 0) == 0
     *                     ...,
     *                     `s$T*B-1` = SUM(x) / (prob/B) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *               BY group
     *             | WHERE `s$0` IS NOT NULL AND ... AND `s$T*B-1` IS NOT NULL
     *             | EVAL t = s*s, `t$0` = `s$0`*`s$0`, ..., `t$T*B-1` = `s$T*B-1`*`s$T*B-1`
     *             | EVAL `CONFIDENCE_INTERVAL(s)` = CONFIDENCE_INTERVAL(s, MV_APPEND(`s$0`, ... `s$T*B-1`), T, B, 0.90),
     *                    `CONFIDENCE_INTERVAL(t)` = CONFIDENCE_INTERVAL(t, MV_APPEND(`t$0`, ... `t$T*B-1`), T, B, 0.90)
     *             | KEEP s, t, `CONFIDENCE_INTERVAL(s)`, `CONFIDENCE_INTERVAL(t)`
     *     }
     * </pre>
     */
    private LogicalPlan approximatePlan(double sampleProbability) {
        if (sampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
            logger.debug("using original plan (too few rows)");
            return logicalPlan;
        }

        logger.debug("generating approximate plan (p={})", sampleProbability);
        Holder<Boolean> encounteredStats = new Holder<>(false);
        Map<NameId, List<Alias>> fieldBuckets = new HashMap<>();

        LogicalPlan approximatePlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                // The leaf plan should be appended by a SAMPLE.
                plan = new Sample(Source.EMPTY, Literal.fromDouble(Source.EMPTY, sampleProbability), plan);

            } else if (encounteredStats.get() == false && plan instanceof Aggregate aggregate) {
                // The STATS function should be replaced by a sample-corrected STATS and buckets
                // to compute confidence intervals.
                encounteredStats.set(true);

                Expression bucketIds = new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT));
                for (int trialId = 1; trialId < TRIAL_COUNT; trialId++) {
                    bucketIds = new MvAppend(
                        Source.EMPTY,
                        bucketIds,
                        new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT))
                    );
                }
                Alias bucketIdField = new Alias(Source.EMPTY, "$bucket_id", bucketIds);

                List<NamedExpression> aggregates = new ArrayList<>();
                Alias sampleSize = new Alias(Source.EMPTY, "$sample_size", COUNT_ALL_ROWS);
                aggregates.add(sampleSize);

                for (NamedExpression aggOrKey : aggregate.aggregates()) {
                    if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                        // This is a grouping key, not an aggregate function.
                        aggregates.add(aggOrKey);
                        continue;
                    }

                    // Replace the original aggregation by a sample-corrected one.
                    Alias aggAlias = (Alias) aggOrKey;
                    AggregateFunction aggFn = (AggregateFunction) aggAlias.child();

                    if (aggFn.equals(COUNT_ALL_ROWS)
                        && aggregate.groupings().isEmpty()
                        && queryProperties.canDecreaseRowCount == false
                        && queryProperties.canIncreaseRowCount == false) {
                        // If the query is preserving all rows, and the aggregation function is
                        // counting all rows, we know the exact result without sampling.
                        aggregates.add(aggAlias.replaceChild(Literal.fromLong(Source.EMPTY, sourceRowCount)));
                        continue;
                    }

                    aggregates.add(aggAlias.replaceChild(correctForSampling(aggFn, sampleProbability)));

                    if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass())) {
                        // For the supported single-valued aggregations, add buckets with sampled
                        // values, that will be used to compute a confidence interval.
                        // For multivalued aggregations, confidence intervals do not make sense.
                        List<Alias> buckets = new ArrayList<>();
                        for (int trialId = 0; trialId < TRIAL_COUNT; trialId++) {
                            for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
                                Expression bucket = correctForSampling(
                                    aggFn.withFilter(
                                        new Equals(
                                            Source.EMPTY,
                                            new MvSlice(
                                                Source.EMPTY,
                                                bucketIdField.toAttribute(),
                                                Literal.integer(Source.EMPTY, trialId),
                                                Literal.integer(Source.EMPTY, trialId)
                                            ),
                                            Literal.integer(Source.EMPTY, bucketId)
                                        )
                                    ),
                                    sampleProbability / BUCKET_COUNT
                                );
                                if (aggFn instanceof Count) {
                                    // For COUNT, no data should result in NULL, like in other aggregations.
                                    bucket = new Case(
                                        Source.EMPTY,
                                        new Equals(Source.EMPTY, bucket, Literal.fromLong(Source.EMPTY, 0L)),
                                        List.of(Literal.NULL, bucket)
                                    );
                                }
                                Alias namedBucket = new Alias(
                                    Source.EMPTY,
                                    aggOrKey.name() + "$" + (trialId * BUCKET_COUNT + bucketId),
                                    bucket
                                );
                                buckets.add(namedBucket);
                                aggregates.add(namedBucket);
                            }
                        }
                        fieldBuckets.put(aggOrKey.id(), buckets);
                    }
                }

                // Add the bucket ID, do the aggregations (sampled corrected, including the buckets),
                // and filter out rows with less than 10 sampled values.
                plan = new Eval(Source.EMPTY, aggregate.child(), List.of(bucketIdField));
                plan = aggregate.with(plan, aggregate.groupings(), aggregates);
                plan = new Filter(
                    Source.EMPTY,
                    plan,
                    new GreaterThanOrEqual(Source.EMPTY, sampleSize.toAttribute(), Literal.fromLong(Source.EMPTY, 10L))
                );

                List<Attribute> keepAttributes = new ArrayList<>(plan.output());
                keepAttributes.remove(sampleSize.toAttribute());
                plan = new Project(Source.EMPTY, plan, keepAttributes);
            } else if (encounteredStats.get()) {
                // After the STATS function, any processing of fields that have buckets, should
                // also process the buckets, so that confidence intervals for the dependent fields
                // can be computed.
                switch (plan) {
                    case Eval eval:
                        // For EVAL, if any of the evaluated expressions depends on a field with buckets,
                        // create corresponding expressions for the buckets as well.
                        // If the results are non-numeric or multivalued, though, confidence intervals don't
                        // apply anymore, and the buckets not computed.
                        List<Alias> fields = new ArrayList<>(eval.fields());
                        for (Alias field : eval.fields()) {
                            // Don't create buckets for non-numeric or multivalued fields.
                            if (field.dataType().isNumeric() == false
                                || field.child().anyMatch(expr -> MULTIVALUED_OUTPUT_FUNCTIONS.contains(expr.getClass()))) {
                                continue;
                            }
                            // If any of the field's dependencies has buckets, create buckets for this field as well.
                            if (field.child().anyMatch(e -> e instanceof NamedExpression ne && fieldBuckets.containsKey(ne.id()))) {
                                List<Alias> buckets = new ArrayList<>();
                                for (int bucketId = 0; bucketId < TRIAL_COUNT * BUCKET_COUNT; bucketId++) {
                                    final int finalBucketId = bucketId;
                                    Expression bucket = field.child()
                                        .transformDown(
                                            e -> e instanceof NamedExpression ne && fieldBuckets.containsKey(ne.id())
                                                ? fieldBuckets.get(ne.id()).get(finalBucketId).toAttribute()
                                                : e
                                        );
                                    buckets.add(new Alias(Source.EMPTY, field.name() + "$" + bucketId, bucket));
                                }
                                fields.addAll(buckets);
                                fieldBuckets.put(field.id(), buckets);
                            }
                        }
                        plan = new Eval(Source.EMPTY, eval.child(), fields);
                        break;

                    case Project project:
                        // For PROJECT, if it renames a fields with buckets, add the renamed field
                        // to the map of fields with buckets.
                        for (NamedExpression projection : project.projections()) {
                            if (projection instanceof Alias alias
                                && alias.child() instanceof NamedExpression named
                                && fieldBuckets.containsKey(named.id())) {
                                fieldBuckets.put(alias.id(), fieldBuckets.get(named.id()));
                            }
                        }
                        break;
                    default:
                }
            }
            return plan;
        });

        Expression constNaN = new Literal(Source.EMPTY, Double.NaN, DataType.DOUBLE);
        Expression trialCount = Literal.integer(Source.EMPTY, TRIAL_COUNT);
        Expression bucketCount = Literal.integer(Source.EMPTY, BUCKET_COUNT);
        Expression confidenceLevel = Literal.fromDouble(Source.EMPTY, CONFIDENCE_LEVEL);

        // Compute the confidence interval for all output fields that have buckets.
        List<Alias> confidenceIntervalsAndReliable = new ArrayList<>();
        for (Attribute output : logicalPlan.output()) {
            if (fieldBuckets.containsKey(output.id())) {
                List<Alias> buckets = fieldBuckets.get(output.id());
                // Collect a multivalued expression with all bucket values, and pass that to the
                // confidence interval computation. Whenever the bucket value is null, replace it
                // by NaN, because multivalued fields cannot have nulls.
                Expression bucketsMv = null;
                for (int i = 0; i < TRIAL_COUNT * BUCKET_COUNT; i++) {
                    Expression bucket = buckets.get(i).toAttribute();
                    if (output.dataType() != DataType.DOUBLE) {
                        bucket = new ToDouble(Source.EMPTY, bucket);
                    }
                    bucket = new Case(Source.EMPTY, new IsNotNull(Source.EMPTY, bucket), List.of(bucket, constNaN));
                    if (bucketsMv == null) {
                        bucketsMv = bucket;
                    } else {
                        bucketsMv = new MvAppend(Source.EMPTY, bucketsMv, bucket);
                    }
                }
                Expression outputDouble = output.dataType() == DataType.DOUBLE ? output : new ToDouble(Source.EMPTY, output);
                Expression confidenceInterval = new ConfidenceInterval(
                    Source.EMPTY,
                    outputDouble,
                    bucketsMv,
                    trialCount,
                    bucketCount,
                    confidenceLevel
                );
                confidenceInterval = switch (output.dataType()) {
                    case DOUBLE -> confidenceInterval;
                    case INTEGER -> new ToInteger(Source.EMPTY, confidenceInterval);
                    case LONG -> new ToLong(Source.EMPTY, confidenceInterval);
                    default -> throw new IllegalStateException("unexpected data type [" + output.dataType() + "]");
                };
                confidenceIntervalsAndReliable.add(
                    new Alias(
                        Source.EMPTY,
                        "CONFIDENCE_INTERVAL(" + output.name() + ")",
                        new MvSlice(Source.EMPTY, confidenceInterval, Literal.integer(Source.EMPTY, 0), Literal.integer(Source.EMPTY, 1))
                    )
                );
                confidenceIntervalsAndReliable.add(
                    new Alias(
                        Source.EMPTY,
                        "RELIABLE(" + output.name() + ")",
                        new GreaterThanOrEqual(
                            Source.EMPTY,
                            new MvSlice(
                                Source.EMPTY,
                                confidenceInterval,
                                Literal.integer(Source.EMPTY, 2),
                                Literal.integer(Source.EMPTY, 2)
                            ),
                            Literal.fromDouble(Source.EMPTY, 0.5)
                        )
                    )
                );
            }
        }
        approximatePlan = new Eval(Source.EMPTY, approximatePlan, confidenceIntervalsAndReliable);

        // Finally, drop all bucket fields from the output.
        Set<Attribute> dropAttributes = fieldBuckets.values()
            .stream()
            .flatMap(List::stream)
            .map(Alias::toAttribute)
            .collect(Collectors.toSet());
        List<Attribute> keepAttributes = new ArrayList<>(approximatePlan.output());
        keepAttributes.removeAll(dropAttributes);
        approximatePlan = new Project(Source.EMPTY, approximatePlan, keepAttributes);

        approximatePlan.setPreOptimized();

        logger.debug("approximate plan:\n{}", approximatePlan);

        return logicalPlanOptimizer.optimize(approximatePlan);
    }

    /**
     * Corrects an aggregation function for random sampling, if needed.
     * Some functions (like COUNT and SUM) need to be scaled up by the inverse of
     * the sampling probability, while others (like AVG and MEDIAN) do not.
     */
    private static Expression correctForSampling(AggregateFunction agg, double sampleProbability) {
        if (SAMPLE_CORRECTED_AGGS.contains(agg.getClass()) == false) {
            return agg;
        }
        Expression correctedAgg = new Div(agg.source(), agg, Literal.fromDouble(Source.EMPTY, sampleProbability));
        return switch (agg.dataType()) {
            case DOUBLE -> correctedAgg;
            case LONG -> new ToLong(agg.source(), correctedAgg);
            default -> throw new IllegalStateException("unexpected data type [" + agg.dataType() + "]");
        };
    }
}
