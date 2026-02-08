/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
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
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class computes approximate results for certain classes of ES|QL queries.
 * Approximate results are usually much faster to compute than exact results.
 * <p>
 * A query is currently suitable for approximation if:
 * <ul>
 *   <li> it contains exactly one {@code STATS} command
 *   <li> the other processing commands are from the supported set
 *        ({@link Approximation#SUPPORTED_COMMANDS}); this set contains almost all
 *        unary commands, but most notably not {@code FORK} or {@code JOIN}.
 *   <li> the aggregate functions are from the supported set
 *        ({@link Approximation#SUPPORTED_SINGLE_VALUED_AGGS} and
 *         {@link Approximation#SUPPORTED_MULTIVALUED_AGGS})
 * </ul>
 * Some of these restrictions may be lifted in the future.
 * <p>
 *
 * When these conditions are met, the query is replaced by an approximation query
 * that samples documents before the STATS, and extrapolates the aggregate
 * functions if needed. This new logical plan is generated by
 * {@link Approximation#approximationPlan}.
 * <p>
 * In addition to approximate results, confidence intervals are also computed.
 * This is done by dividing the sampled rows {@link Approximation#TRIAL_COUNT}
 * times into {@link Approximation#BUCKET_COUNT} buckets, computing the aggregate
 * functions for each bucket, and using these sampled values to compute
 * confidence intervals.
 * <p>
 * To obtain an appropriate sample probability, first a target number of rows
 * is set. For now this is determined via the ({@link ApproximationSettings}).
 * Next, the total number of rows in the source index is counted via the plan
 * {@link Approximation#sourceCountPlan}. This plan always executes fast. When
 * there are no commands that can change the number of rows, the sample
 * probability can be directly computed as a ratio of the target number of rows
 * and this total number.
 * <p>
 * In the presence of commands that can change the number of rows, another step
 * is needed. The first goal is to find a sample probability that leads to
 * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} rows, and when is
 * probability is found, a sample probability leading to the target number of
 * row is computed.
 * <p>
 * This is done by setting the initial sample probability to the ratio of
 * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} and the total number
 * of rows in the source index, and a number of rows is sampled with the plan
 * {@link Approximation#countPlan}. As long as the sampled number of rows is too
 * small, the probability is scaled up until a good probability is reached. This
 * final probability is used to compute the probability using for approximating
 * the original query.
 */
public class Approximation {

    public record QueryProperties(boolean hasGrouping, boolean canDecreaseRowCount, boolean canIncreaseRowCount) {}

    /**
     * These processing commands are supported.
     * <p>
     * When a command is not supported, it should be added to
     * ApproximationSupportTests.UNSUPPORTED_COMMANDS
     * to make sure all commands are captured.
     */
    static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS = Set.of(
        Aggregate.class,
        ChangePoint.class,
        Completion.class,
        Dissect.class,
        Enrich.class,
        EsRelation.class,
        Eval.class,
        Filter.class,
        Grok.class,
        Insist.class,
        Limit.class,
        MvExpand.class,
        OrderBy.class,
        Project.class,
        RegexExtract.class,
        Rerank.class,
        Row.class,
        Sample.class,
        TopN.class,
        UriParts.class
    );

    /**
     * These index modes of EsRelation are supported.
     * <p>
     * Note: LOOKUP is added to make query validation output nicer messages
     * ("query with [LOOKUP JOIN ...] ..." instead of "query with [test_lookup] ...").
     */
    private static final Set<IndexMode> SUPPORTED_INDEX_MODES = Set.of(IndexMode.STANDARD, IndexMode.LOOKUP);

    /**
     * These commands preserve all rows, making it easy to predict the number of output rows.
     */
    private static final Set<Class<? extends LogicalPlan>> ROW_PRESERVING_COMMANDS = Set.of(
        Completion.class,
        Dissect.class,
        Enrich.class,
        Eval.class,
        Grok.class,
        Insist.class,
        OrderBy.class,
        Project.class,
        RegexExtract.class,
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
     * <p>
     * Aggregation functions that depend on the value of many or all of the
     * individual rows (like AVG, COUNT, MEDIAN) will generally satisfy the CLT
     * and are suitable for approximation.
     * On the other hand, aggregation functions that depend on a small subset of
     * rows (like COUNT DISTINCT, MAX, PRESENT) are not.
     * Aggregation function that depend on all values, but are very sensitive to
     * some (outlier) values (like STDDEV, PERCENTILE for low/high percentiles)
     * are challenging to approximate. They are supported, but may require a
     * larger sample size to obtain good accuracy.
     * <p>
     * When an aggregation is not supported, it should be added to
     * ApproximationSupportTests.UNSUPPORTED_AGGS
     * to make sure all aggregations are captured.
     */
    static final Set<Class<? extends AggregateFunction>> SUPPORTED_SINGLE_VALUED_AGGS = Set.of(
        Avg.class,
        Count.class,
        Median.class,
        MedianAbsoluteDeviation.class,
        Percentile.class,
        StdDev.class,
        Sum.class,
        Variance.class,
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
    static final Set<Class<? extends AggregateFunction>> SUPPORTED_MULTIVALUED_AGGS = Set.of(
        org.elasticsearch.xpack.esql.expression.function.aggregate.Sample.class
    );

    /**
     * These numerical scalar functions produce multivalued output. This means that
     * confidence intervals cannot be computed anymore and are dropped.
     * <p>
     * Numerical scalar functions that produce multivalued output should be added
     * here. Forgetting to do so leads to confidence intervals columns for the
     * multivalued fields, that are filled with nulls.
     */
    private static final Set<Class<? extends EsqlScalarFunction>> MULTIVALUED_OUTPUT_FUNCTIONS = Set.of(MvAppend.class);

    /**
     * The target number of sampled rows to estimate the total number of rows
     * reaching the STATS function. This is only relevant when there are commands
     * that can change the number of rows. This value leads to an error of about
     * 1% in the estimated count.
     */
    private static final int ROW_COUNT_FOR_COUNT_ESTIMATION = 10_000;

    /**
     * Default number of rows to sample for approximation without grouping.
     * 100_000 rows is enough to accurately estimate most single aggregates.
     */
    private static final int DEFAULT_ROW_COUNT_WITHOUT_GROUPING = 100_000;

    /**
     * Default number of rows to sample for approximation with grouping.
     * To accurately estimate aggregates for each group, more rows are needed.
     */
    private static final int DEFAULT_ROW_COUNT_WITH_GROUPING = 1_000_000;

    /**
     * Default confidence level for confidence intervals.
     */
    private static final double DEFAULT_CONFIDENCE_LEVEL = 0.90;

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

    /**
     * For grouped statistics (STATS ... BY), a grouping needs at least this
     * number of sampled rows to be included in the results. For a simple
     * aggregation as count, this still leads to acceptable confidence
     * intervals. This will never be marked as "certified reliable".
     */
    private static final int MIN_ROW_COUNT_FOR_RESULT_INCLUSION = 10;

    private static final Logger logger = LogManager.getLogger(Approximation.class);

    private static final AggregateFunction COUNT_ALL_ROWS = new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, StringUtils.WILDCARD));

    private final LogicalPlan logicalPlan;
    private final ApproximationSettings settings;
    private final EsqlExecutionInfo executionInfo;
    private final QueryProperties queryProperties;
    private final EsqlSession.PlanRunner runner;
    private final Function<LogicalPlan, PhysicalPlan> toPhysicalPlan;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final TransportVersion minimumVersion;
    private final PlanTimeProfile planTimeProfile;

    private final SetOnce<Long> sourceRowCount;
    private int countRecursionDepth;

    public Approximation(
        LogicalPlan logicalPlan,
        ApproximationSettings settings,
        EsqlExecutionInfo executionInfo,
        Function<LogicalPlan, PhysicalPlan> toPhysicalPlan,
        EsqlSession.PlanRunner runner,
        Configuration configuration,
        FoldContext foldContext,
        TransportVersion minimumVersion,
        PlanTimeProfile planTimeProfile
    ) {
        this.logicalPlan = logicalPlan;
        this.settings = settings;
        this.executionInfo = executionInfo;
        this.queryProperties = verifyPlan(logicalPlan);
        this.toPhysicalPlan = toPhysicalPlan;
        this.runner = runner;
        this.configuration = configuration;
        this.foldContext = foldContext;
        this.minimumVersion = minimumVersion;
        this.planTimeProfile = planTimeProfile;

        sourceRowCount = new SetOnce<>();
        countRecursionDepth = 0;
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
            if (SUPPORTED_COMMANDS.contains(plan.getClass()) == false
                || (plan instanceof EsRelation esRelation && SUPPORTED_INDEX_MODES.contains(esRelation.indexMode()) == false)) {
                // TODO: ideally just return the command from the source
                throw new VerificationException(
                    List.of(Failure.fail(plan, "query with [" + plan.sourceText() + "] cannot be approximated"))
                );
            }
        });

        Holder<Boolean> encounteredStats = new Holder<>(false);
        Holder<Boolean> hasGrouping = new Holder<>();
        Holder<Boolean> canIncreaseRowCount = new Holder<>(false);
        Holder<Boolean> canDecreaseRowCount = new Holder<>(false);

        logicalPlan.forEachUp(plan -> {
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    // Verify that the aggregate functions are supported.
                    encounteredStats.set(true);
                    hasGrouping.set(aggregate.groupings().isEmpty() == false);
                    plan.forEachExpression(AggregateFunction.class, aggFn -> {
                        if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass()) == false
                            && SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
                            // TODO: ideally just return aggregate function from the source
                            throw new VerificationException(
                                List.of(Failure.fail(aggFn, "aggregation function [" + aggFn.sourceText() + "] cannot be approximated"))
                            );
                        }
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
        });

        return new QueryProperties(hasGrouping.get(), canDecreaseRowCount.get(), canIncreaseRowCount.get());
    }

    /**
     * Computes approximate results for the logical plan.
     */
    public void approximate(ActionListener<Result> listener) {
        // TODO: check if the plan can be translated to an EsStatsQuery.
        // If so, don't query approximation, because it's slower.
        executionInfo.startSubPlans();
        runner.run(toPhysicalPlan.apply(sourceCountPlan()), configuration, foldContext, planTimeProfile, sourceCountListener(listener));
    }

    /**
     * Returns the original logical plan (returning the exact results), extended with the
     * confidence interval and certified fields, that the approximation plan would have
     * returned. The confidence interval of a field is [exact_value, exact_value] and the
     * certified field is always true.
     */
    private LogicalPlan exactPlanWithConfidenceIntervals() {
        Map<String, Attribute> exactFields = logicalPlan.output()
            .stream()
            .collect(Collectors.toMap(NamedExpression::name, Function.identity()));

        // Compute the approximation plan to find out which fields need confidence intervals.
        // To do so, use a non-zero sample probability lower than the threshold. Otherwise,
        // the approximation plan uses this exactPlanWithConfidenceIntervals again.
        LogicalPlan approximationPlan = approximationPlan(SAMPLE_PROBABILITY_THRESHOLD / 2.0);
        List<Alias> confidenceIntervalsAndCertified = new ArrayList<>();
        for (Attribute field : approximationPlan.output()) {
            // TODO: handle user-defined fields that collide with the extra fields.
            if (exactFields.containsKey(field.name())) {
                continue;
            }
            if (field.name().startsWith("CONFIDENCE_INTERVAL")) {
                String exactFieldName = field.name().substring("CONFIDENCE_INTERVAL(".length(), field.name().length() - 1);
                Attribute exactField = exactFields.get(exactFieldName);
                confidenceIntervalsAndCertified.add(
                    new Alias(Source.EMPTY, field.name(), new MvAppend(Source.EMPTY, exactField, exactField))
                );
            }
            if (field.name().startsWith("CERTIFIED")) {
                confidenceIntervalsAndCertified.add(new Alias(Source.EMPTY, field.name(), Literal.TRUE));
            }
        }
        // The approximation plan appends the confidence interval and certified fields
        // at the end, so do the same here.
        LogicalPlan logicalPlanWithConfidenceIntervals = new Eval(Source.EMPTY, logicalPlan, confidenceIntervalsAndCertified);
        logicalPlanWithConfidenceIntervals.setOptimized();
        return logicalPlanWithConfidenceIntervals;
    }

    private int sampleRowCount() {
        if (settings.rows() != null) {
            return settings.rows();
        } else if (queryProperties.hasGrouping) {
            return DEFAULT_ROW_COUNT_WITH_GROUPING;
        } else {
            return DEFAULT_ROW_COUNT_WITHOUT_GROUPING;
        }
    }

    private double confidenceLevel() {
        return settings.confidenceLevel() != null ? settings.confidenceLevel() : DEFAULT_CONFIDENCE_LEVEL;
    }

    /**
     * Plan that counts the number of rows in the source index.
     * This is the ES|QL query:
     * <pre>
     * {@code FROM index | STATS COUNT(*)}
     * </pre>
     */
    private LogicalPlan sourceCountPlan() {
        LogicalPlan leaf = logicalPlan.collectLeaves().getFirst();
        LogicalPlan sourceCountPlan = new Aggregate(
            Source.EMPTY,
            leaf,
            List.of(),
            List.of(new Alias(Source.EMPTY, "$count", COUNT_ALL_ROWS))
        );
        sourceCountPlan.setOptimized();
        return sourceCountPlan;
    }

    /**
     * Receives the total number of rows, and runs either the
     * {@link Approximation#approximationPlan} or {@link Approximation#countPlan}
     * depending on whether the original query preserves all rows or not.
     */
    private ActionListener<Result> sourceCountListener(ActionListener<Result> approximationListener) {
        return approximationListener.delegateFailureAndWrap((listener, countResult) -> {
            sourceRowCount.set(rowCount(countResult));
            logger.debug("total number of source rows: [{}] rows", sourceRowCount);
            if (sourceRowCount.get() == 0) {
                // If there are no rows, run the original query.
                executionInfo.finishSubPlans();
                runner.run(toPhysicalPlan.apply(exactPlanWithConfidenceIntervals()), configuration, foldContext, planTimeProfile, listener);
                return;
            }
            double sampleProbability = Math.min(1.0, (double) sampleRowCount() / sourceRowCount.get());
            if (queryProperties.canIncreaseRowCount == false && queryProperties.canDecreaseRowCount == false) {
                // If the query preserves all rows, we can directly approximate with the sample probability.
                executionInfo.finishSubPlans();
                runner.run(
                    toPhysicalPlan.apply(approximationPlan(sampleProbability)),
                    configuration,
                    foldContext,
                    planTimeProfile,
                    listener
                );
            } else if (queryProperties.canIncreaseRowCount == false && sampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
                // If the query cannot increase the number of rows, and the sample probability is large,
                // we can directly run the original query without sampling.
                logger.debug("using original plan (too few rows)");
                executionInfo.finishSubPlans();
                runner.run(toPhysicalPlan.apply(exactPlanWithConfidenceIntervals()), configuration, foldContext, planTimeProfile, listener);
            } else {
                // Otherwise, we need to sample the number of rows first to obtain a good sample probability.
                sampleProbability = Math.min(1.0, (double) ROW_COUNT_FOR_COUNT_ESTIMATION / sourceRowCount.get());
                runner.run(
                    toPhysicalPlan.apply(countPlan(sampleProbability)),
                    configuration,
                    foldContext,
                    planTimeProfile,
                    countListener(sampleProbability, listener)
                );
            }
        });
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

        countPlan.setOptimized();
        return countPlan;
    }

    /**
     * Receives the sampled number of rows reaching the STATS function.
     * Runs either the {@link Approximation#approximationPlan} or a next iteration
     * {@link Approximation#countPlan} depending on whether the current count is
     * sufficient.
     * <p>
     * This count listener works recursively, increasing the sample probability at
     * each step. The first iteration uses a probability of
     * <pre>
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} / {@link Approximation#sourceRowCount}
     * </pre>
     * which is set by the {@link Approximation#sourceCountListener}.
     * <p>
     * If the sampled row count is high enough, the approximation plan is run next.
     * <p>
     * If the sampled row count is low the probability is multiplied by
     * <pre>
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} / Math.max(1, rowCount)
     * </pre>
     * and the count plan is run again.
     * <p>
     * For positive rowCount this means the next iteration aims for
     * approximately {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} rows.
     * The recursion stops when a rowCount larger than {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} / 2
     * is encountered. The division by 2 is to stop when the random sampling
     * returns a few less rows than aimed for.
     * <p>
     * For zero rowCount the sample probability is multiplied by
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION}. That means the next
     * recursion step can sample a number of rows between 0 and the target count,
     * but can never overshoot by much.
     * <p>
     * Regarding the recursion depth: in the worst case (very large index, very few
     * matching docs), the first few iterations return zero rows, and the sample probability
     * is multiplied by {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} each time.
     * Next, there's one iteration that returns a positive small row count, and the
     * probability is scaled up the correct value. Next, there's one final iteration
     * with approximately {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} rows.
     * <p>
     * This result in a maximum recursion depth of about:
     * <pre>
     *     log(sourceRowCount, {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION}) + 2
     * </pre>
     * which is at most 7 when sourceRowCount=MAX_LONG (huge!) and
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} = 10000.
     * To be safe, the maximum recursion depth is capped at 10, and an exception is thrown
     * when this depth is exceeded.
     */
    private ActionListener<Result> countListener(double sampleProbability, ActionListener<Result> approximationListener) {
        return approximationListener.delegateFailureAndWrap((listener, countResult) -> {
            countRecursionDepth += 1;
            if (countRecursionDepth > 10) {
                listener.onFailure(new IllegalStateException("Approximation count recursion limit exceeded"));
                return;
            }
            long rowCount = rowCount(countResult);
            logger.debug("estimated number of rows reaching STATS (p=[{}]): [{}] rows", sampleProbability, rowCount);
            double newSampleProbability = Math.min(1.0, sampleProbability * sampleRowCount() / Math.max(1, rowCount));
            if (newSampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
                // If the new sample probability is large, run the original query.
                logger.debug("using original plan (too few rows)");
                executionInfo.finishSubPlans();
                runner.run(toPhysicalPlan.apply(exactPlanWithConfidenceIntervals()), configuration, foldContext, planTimeProfile, listener);
            } else if (rowCount <= ROW_COUNT_FOR_COUNT_ESTIMATION / 2) {
                // Not enough rows are sampled yet; increase the sample probability and try again.
                newSampleProbability = Math.min(1.0, sampleProbability * ROW_COUNT_FOR_COUNT_ESTIMATION / Math.max(1, rowCount));
                runner.run(
                    toPhysicalPlan.apply(countPlan(newSampleProbability)),
                    configuration,
                    foldContext,
                    planTimeProfile,
                    countListener(newSampleProbability, listener)
                );
            } else {
                executionInfo.finishSubPlans();
                runner.run(
                    toPhysicalPlan.apply(approximationPlan(newSampleProbability)),
                    configuration,
                    foldContext,
                    planTimeProfile,
                    listener
                );
            }
        });
    }

    /**
     * Returns the row count in the result and closes the result.
     */
    private long rowCount(Result countResult) {
        assert countResult.pages().size() == 1;
        assert countResult.pages().getFirst().getBlockCount() == 1;
        assert countResult.pages().getFirst().getPositionCount() == 1;

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
     *              <li> {@code COUNT} to track the sample size
     *              <li> Each aggregate function replaced by a sample-corrected version (if needed)
     *              <li> {@link Approximation#TRIAL_COUNT} * {@link Approximation#BUCKET_COUNT} additional columns
     *                   with a sampled values for each aggregate function, sample-corrected (if needed)
     *          </ul>
     *     <li> {@code FILTER} to remove all rows with a too small sample size
     *     <li> All commands after the {@code STATS} command, modified to also process
     *          the additional bucket columns where possible
     *     <li> {@code EVAL} to compute confidence intervals for all fields with buckets
     *     <li> {@code PROJECT} to drop all non-output columns
     * </ul>
     *
     * As an example, the simple query:
     * <pre>
     *     {@code
     *         FROM index
     *             | EVAL x = 2*x
     *             | STATS s = SUM(x) BY group
     *             | EVAL t = s*s
     *     }
     * </pre>
     * is rewritten to (prob=sampleProbability, T=trialCount, B=bucketCount):
     * <pre>
     *     {@code
     *         FROM index
     *             | SAMPLE prob
     *             | EVAL x = 2*x
     *             | EVAL bucketId = MV_APPEND(RANDOM(B), ... , RANDOM(B))  // T times
     *             | STATS sampleSize = COUNT(*),
     *                     s = SUM(x) / prob,
     *                     `s$0` = SUM(x) / (prob/B)) WHERE MV_SLICE(bucketId, 0, 0) == 0
     *                     ...,
     *                     `s$T*B-1` = SUM(x) / (prob/B) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *               BY group
     *             | WHERE sampleSize >= 10
     *             | EVAL t = s*s, `t$0` = `s$0`*`s$0`, ..., `t$T*B-1` = `s$T*B-1`*`s$T*B-1`
     *             | EVAL `CONFIDENCE_INTERVAL(s)` = CONFIDENCE_INTERVAL(s, MV_APPEND(`s$0`, ... `s$T*B-1`), T, B, 0.90),
     *                    `CONFIDENCE_INTERVAL(t)` = CONFIDENCE_INTERVAL(t, MV_APPEND(`t$0`, ... `t$T*B-1`), T, B, 0.90)
     *             | KEEP s, t, `CONFIDENCE_INTERVAL(s)`, `CONFIDENCE_INTERVAL(t)`
     *     }
     * </pre>
     */
    private LogicalPlan approximationPlan(double sampleProbability) {
        if (sampleProbability > SAMPLE_PROBABILITY_THRESHOLD) {
            logger.debug("using original plan (too few rows)");
            return exactPlanWithConfidenceIntervals();
        }

        logger.debug("generating approximation plan (p=[{}])", sampleProbability);

        // Whether of not the first STATS command has been encountered yet.
        Holder<Boolean> encounteredStats = new Holder<>(false);

        // The keys are the IDs of the fields that have buckets. Confidence intervals are computed
        // for these fields at the end of the computation. They map to the list of buckets for
        // that field.
        Map<NameId, List<Alias>> fieldBuckets = new HashMap<>();

        // For each sample-corrected expression, also keep track of the uncorrected expression.
        // These are used when a division between two sample-corrected expressions is encountered.
        // This results in the same value (because (expr1/prob) / (expr2/prob) == expr1/expr2),
        // except that no round-off errors occur if either the numerator or denominator is an
        // integer and rounded to that after sample-correction. The most common case is AVG, which
        // is rewritten to AVG::double = SUM::double / COUNT::long.
        Map<NameId, NamedExpression> uncorrectedExpressions = new HashMap<>();

        LogicalPlan approximationPlan = logicalPlan.transformUp(plan -> {
            if (plan instanceof LeafPlan) {
                // The leaf plan should be appended by a SAMPLE.
                return new Sample(Source.EMPTY, Literal.fromDouble(Source.EMPTY, sampleProbability), plan);
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate == false) {
                    // Commands before the first STATS function should be left unchanged.
                    return plan;
                } else {
                    // The first STATS function should be replaced by a sample-corrected STATS
                    // and buckets (for computing confidence intervals).
                    encounteredStats.set(true);
                    return sampleCorrectedAggregateAndBuckets((Aggregate) plan, sampleProbability, fieldBuckets, uncorrectedExpressions);
                }
            } else {
                // After the STATS function, any processing of fields that have buckets, should
                // also process the buckets, so that confidence intervals for the dependent fields
                // can be computed.
                return planIncludingBuckets(plan, fieldBuckets, uncorrectedExpressions);
            }
        });

        // Add the confidence intervals for all fields with buckets.
        approximationPlan = new Eval(Source.EMPTY, approximationPlan, getConfidenceIntervals(fieldBuckets));

        // Drop all bucket fields and uncorrected fields from the output.
        Set<Attribute> dropAttributes = Stream.concat(
            fieldBuckets.values().stream().flatMap(List::stream),
            uncorrectedExpressions.values().stream()
        ).map(NamedExpression::toAttribute).collect(Collectors.toSet());

        List<Attribute> keepAttributes = new ArrayList<>(approximationPlan.output());
        keepAttributes.removeAll(dropAttributes);
        approximationPlan = new Project(Source.EMPTY, approximationPlan, keepAttributes);
        approximationPlan = optimize(approximationPlan);
        logger.debug("approximation plan (after:\n{}", approximationPlan);
        return approximationPlan;
    }

    /**
     * Optimizes the plan by running just the operator batch. This is primarily
     * to prune unnecessary columns generated in the approximation plan.
     * <p>
     * These unnecessary columns are generated in various ways, for example:
     * - STATS AVG(x): the AVG is rewritten via a surrogate to SUM and COUNT.
     *   Both the corrected and uncorrected sum and count column are generated,
     *   but the corrected ones aren't needed for AVG(x) = SUM(x)/COUNT().
     * - STATS x=COUNT() | EVAL x=TO_STRING(x): bucket columns are generated
     *   for the numeric count, but are not needed after the string conversion.
     * <p>
     * Note this is running the operator batch on top of a plan that it normally
     * doesn't run on (namely a cleaned-up plan, which can contain a TopN). It
     * seems like that works (at least for everything we've tested), but this
     * process has poor test coverage.
     * TODO: refactor so that approximation ideally happens halfway-through
     * optimization, before cleanup step. Possibly make approximation an
     * optimization rule itself in the substitutions batch.
     */
    private LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(configuration, foldContext, minimumVersion)) {
            @Override
            protected List<Batch<LogicalPlan>> batches() {
                return List.of(operators());
            }
        };
        return optimizer.optimize(plan);
    }

    /**
     * Replaces the aggregate by a sample-corrected aggregate and buckets, and
     * filters out groups with a too small sample size. This means that:
     * <pre>
     *     {@code
     *          STATS s = SUM(x) BY group
     *     }
     * </pre>
     * is replaced by:
     * <pre>
     *     {@code
     *          STATS sampleSize = COUNT(*),
     *                s = SUM(x),
     *                `s$0` = SUM(x) WHERE MV_SLICE(bucketId, 0, 0) == 0
     *                ...,
     *                `s$T*B-1` = SUM(x) / (prob/B) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *          BY group
     *          | WHERE sampleSize >= MIN_ROW_COUNT_FOR_RESULT_INCLUSION
     *          | EVAL s = s / prob, `s$0` = `s$0` / (prob/B), `s$T*B-1` = `s$T*B-1` / (prob/B)
     *          | DROP sampleSize
     *      }
     * </pre>
     */
    private LogicalPlan sampleCorrectedAggregateAndBuckets(
        Aggregate aggregate,
        double sampleProbability,
        Map<NameId, List<Alias>> fieldBuckets,
        Map<NameId, NamedExpression> uncorrectedExpressions
    ) {
        Expression randomBucketId = new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT));
        Expression bucketIds = randomBucketId;
        for (int trialId = 1; trialId < TRIAL_COUNT; trialId++) {
            bucketIds = new MvAppend(Source.EMPTY, bucketIds, randomBucketId);
        }
        // TODO: use theoretically non-conflicting names.
        Alias bucketIdField = new Alias(Source.EMPTY, "$bucket_id", bucketIds);

        // The aggregate functions in the approximation plan.
        List<NamedExpression> aggregates = new ArrayList<>();

        // List of expressions that must be evaluated after the sampled aggregation.
        // These consist of:
        // - sample corrections (to correct counts/sums for sampling)
        // - replace zero counts by NULLs (for confidence interval computation)
        // - exact total row count if COUNT(*) is used (to avoid sampling errors there)
        List<Alias> evals = new ArrayList<>();

        for (NamedExpression aggOrKey : aggregate.aggregates()) {
            if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                // This is a grouping key, not an aggregate function.
                aggregates.add(aggOrKey);
                continue;
            }

            Alias aggAlias = (Alias) aggOrKey;
            AggregateFunction aggFn = (AggregateFunction) aggAlias.child();

            // If the query is preserving all rows, and the aggregation function is
            // counting all rows, we know the exact result without sampling.
            if (aggFn.equals(COUNT_ALL_ROWS)
                && aggregate.groupings().isEmpty()
                && queryProperties.canDecreaseRowCount == false
                && queryProperties.canIncreaseRowCount == false) {
                evals.add(aggAlias.replaceChild(Literal.fromLong(Source.EMPTY, sourceRowCount.get())));
                continue;
            }

            // Replace the original aggregation by a sample-corrected one if needed.
            if (SAMPLE_CORRECTED_AGGS.contains(aggFn.getClass()) == false) {
                aggregates.add(aggAlias);
            } else {
                Alias uncorrectedAggAlias = new Alias(aggAlias.source(), aggAlias.name() + "$uncorrected", aggFn);
                aggregates.add(uncorrectedAggAlias);
                uncorrectedExpressions.put(aggAlias.id(), uncorrectedAggAlias);

                Expression correctedAgg = correctForSampling(uncorrectedAggAlias.toAttribute(), sampleProbability);
                evals.add(aggAlias.replaceChild(correctedAgg));
            }

            if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass())) {
                // For the supported single-valued aggregations, add buckets with sampled
                // values, that will be used to compute a confidence interval.
                // For multivalued aggregations, confidence intervals do not make sense.
                List<Alias> buckets = new ArrayList<>();
                for (int trialId = 0; trialId < TRIAL_COUNT; trialId++) {
                    for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
                        Expression bucket = aggFn.withFilter(
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
                        );
                        Alias bucketAlias = new Alias(Source.EMPTY, aggOrKey.name() + "$" + (trialId * BUCKET_COUNT + bucketId), bucket);
                        if (SAMPLE_CORRECTED_AGGS.contains(aggFn.getClass()) == false) {
                            buckets.add(bucketAlias);
                            aggregates.add(bucketAlias);
                        } else {
                            Alias uncorrectedBucketAlias = new Alias(Source.EMPTY, bucketAlias.name() + "$uncorrected", bucket);
                            aggregates.add(uncorrectedBucketAlias);
                            uncorrectedExpressions.put(bucketAlias.id(), uncorrectedBucketAlias);

                            Expression uncorrectedBucket = uncorrectedBucketAlias.toAttribute();
                            if (aggFn.equals(COUNT_ALL_ROWS)) {
                                // For COUNT, no data should result in NULL, like in other aggregations.
                                // Otherwise, the confidence interval computation breaks.
                                uncorrectedBucket = new Case(
                                    Source.EMPTY,
                                    new Equals(Source.EMPTY, uncorrectedBucket, Literal.fromLong(Source.EMPTY, 0L)),
                                    List.of(Literal.NULL, uncorrectedBucket)
                                );
                            }

                            Expression correctedBucket = correctForSampling(uncorrectedBucket, sampleProbability / BUCKET_COUNT);
                            Alias correctedBucketAlias = bucketAlias.replaceChild(correctedBucket);
                            evals.add(correctedBucketAlias);
                            buckets.add(correctedBucketAlias);
                        }
                    }
                }
                fieldBuckets.put(aggOrKey.id(), buckets);
            }
        }

        // Add the sample size per grouping to filter out groups with too few sampled rows.
        Alias sampleSize = new Alias(Source.EMPTY, "$sample_size", COUNT_ALL_ROWS);
        aggregates.add(sampleSize);

        // Add the bucket ID, do the aggregations (sampled corrected, including the buckets),
        // and filter out rows with too few sampled values.
        LogicalPlan plan = new Eval(Source.EMPTY, aggregate.child(), List.of(bucketIdField));
        plan = aggregate.with(plan, aggregate.groupings(), aggregates);
        plan = new Filter(
            Source.EMPTY,
            plan,
            new GreaterThanOrEqual(
                Source.EMPTY,
                sampleSize.toAttribute(),
                Literal.integer(Source.EMPTY, MIN_ROW_COUNT_FOR_RESULT_INCLUSION)
            )
        );
        plan = new Eval(Source.EMPTY, plan, evals);

        List<Attribute> keepAttributes = new ArrayList<>(plan.output());
        keepAttributes.remove(sampleSize.toAttribute());
        return new Project(Source.EMPTY, plan, keepAttributes);
    }

    /**
     * Corrects an aggregation function for random sampling.
     * Some functions (like COUNT and SUM) need to be scaled up by the inverse of
     * the sampling probability, while others (like AVG and MEDIAN) do not.
     */
    private static Expression correctForSampling(Expression expr, double sampleProbability) {
        Expression correctedAgg = new Div(expr.source(), expr, Literal.fromDouble(Source.EMPTY, sampleProbability));
        return switch (expr.dataType()) {
            case DOUBLE -> correctedAgg;
            case LONG -> new ToLong(expr.source(), correctedAgg);
            default -> throw new IllegalStateException("unexpected data type [" + expr.dataType() + "]");
        };
    }

    /**
     * Returns a plan that also processes the buckets for fields that have them.
     * Luckily, there's only a limited set of commands that have to do something
     * with the buckets: EVAL, PROJECT and MV_EXPAND.
     */
    private LogicalPlan planIncludingBuckets(
        LogicalPlan plan,
        Map<NameId, List<Alias>> fieldBuckets,
        Map<NameId, NamedExpression> uncorrectedExpressions
    ) {
        return switch (plan) {
            case Eval eval -> evalIncludingBuckets(eval, fieldBuckets, uncorrectedExpressions);
            case Project project -> projectIncludingBuckets(project, fieldBuckets);
            case MvExpand mvExpand -> mvExpandIncludingBuckets(mvExpand, fieldBuckets);
            default -> plan;
        };
    }

    /**
     * For EVAL, if any of the evaluated expressions depends on a field with buckets,
     * create corresponding expressions for the buckets as well. If the results are
     * non-numeric or multivalued, though, confidence intervals don't apply anymore,
     * and the buckets not computed.
     */
    private LogicalPlan evalIncludingBuckets(
        Eval eval,
        Map<NameId, List<Alias>> fieldBuckets,
        Map<NameId, NamedExpression> uncorrectedExpressions
    ) {
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
        // For each division of two sample-corrected expressions, replace it by
        // a division of the corresponding uncorrected expressions.
        for (int i = 0; i < fields.size(); i++) {
            Alias field = fields.get(i);
            fields.set(i, field.replaceChild(field.child().transformUp(e -> {
                if (e instanceof Div div
                    && div.left() instanceof NamedExpression left
                    && uncorrectedExpressions.containsKey(left.id())
                    && div.right() instanceof NamedExpression right
                    && uncorrectedExpressions.containsKey(right.id())) {
                    return new Div(
                        e.source(),
                        uncorrectedExpressions.get(left.id()).toAttribute(),
                        uncorrectedExpressions.get(right.id()).toAttribute(),
                        div.dataType()
                    );
                }
                return e;
            })));
        }
        return new Eval(Source.EMPTY, eval.child(), fields);
    }

    /**
     * For PROJECT, if it renames a field with buckets, add the renamed field
     * to the map of fields with buckets.
     */
    private LogicalPlan projectIncludingBuckets(Project project, Map<NameId, List<Alias>> fieldBuckets) {
        for (NamedExpression projection : project.projections()) {
            if (projection instanceof Alias alias
                && alias.child() instanceof NamedExpression named
                && fieldBuckets.containsKey(named.id())) {
                fieldBuckets.put(alias.id(), fieldBuckets.get(named.id()));
            }
        }

        // When PROJECT keeps a field with buckets, also keep the buckets.
        List<NamedExpression> projections = null;
        for (NamedExpression projection : project.projections()) {
            if (fieldBuckets.containsKey(projection.id())) {
                if (projections == null) {
                    projections = new ArrayList<>(project.projections());
                }
                for (Alias bucket : fieldBuckets.get(projection.id())) {
                    projections.add(bucket.toAttribute());
                }
            }
        }
        if (projections != null) {
            project = project.withProjections(projections);
        }
        return project;
    }

    /**
     * Fields with buckets are always single-valued, so expanding them doesn't
     * do anything and the buckets of the expanded field are the same as those
     * of the target field.
     */
    private LogicalPlan mvExpandIncludingBuckets(MvExpand mvExpand, Map<NameId, List<Alias>> fieldBuckets) {
        if (fieldBuckets.containsKey(mvExpand.target().id())) {
            fieldBuckets.put(mvExpand.expanded().id(), fieldBuckets.get(mvExpand.target().id()));
        }
        return mvExpand;
    }

    /**
     * Returns the confidence interval and certified fields for the fields.
     * This is the expression:
     * <pre>
     *     {@code
     *         CONFIDENCE_INTERVAL(s, MV_APPEND(`s$0`, ... `s$T*B-1`), T, B, 0.90)
     *     }
     * </pre>
     * for each field {@code s} that has buckets. The output of {@code CONFIDENCE_INTERVAL}
     * is separated into two fields: the confidence interval itself, and a certified field.
     */
    private List<Alias> getConfidenceIntervals(Map<NameId, List<Alias>> fieldBuckets) {
        Expression constNaN = new Literal(Source.EMPTY, Double.NaN, DataType.DOUBLE);
        Expression trialCount = Literal.integer(Source.EMPTY, TRIAL_COUNT);
        Expression bucketCount = Literal.integer(Source.EMPTY, BUCKET_COUNT);
        Expression confidenceLevel = Literal.fromDouble(Source.EMPTY, confidenceLevel());

        // Compute the confidence interval for all output fields that have buckets.
        List<Alias> confidenceIntervalsAndCertified = new ArrayList<>();
        for (Attribute output : logicalPlan.output()) {
            if (fieldBuckets.containsKey(output.id())) {
                List<Alias> buckets = fieldBuckets.get(output.id());
                // Collect a multivalued expression with all bucket values, and pass that to the
                // confidence interval computation. Whenever the bucket value is null, replace it
                // by NaN, because multivalued fields cannot have nulls.
                // This is a bit of a back, because ES|QL generally does not support NaN values,
                // but these values stay inside here and the confidence interval computation, and
                // never reach the user.
                // TODO: don't use NaNs, perhaps when nulls in multivalued are supported, see:
                // https://github.com/elastic/elasticsearch/issues/141383
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
                confidenceIntervalsAndCertified.add(
                    new Alias(
                        Source.EMPTY,
                        "CONFIDENCE_INTERVAL(" + output.name() + ")",
                        new MvSlice(Source.EMPTY, confidenceInterval, Literal.integer(Source.EMPTY, 0), Literal.integer(Source.EMPTY, 1))
                    )
                );
                confidenceIntervalsAndCertified.add(
                    new Alias(
                        Source.EMPTY,
                        "CERTIFIED(" + output.name() + ")",
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
        return confidenceIntervalsAndCertified;
    }
}
