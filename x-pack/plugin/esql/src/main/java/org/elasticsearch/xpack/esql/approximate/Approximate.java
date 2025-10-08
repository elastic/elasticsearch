/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Reliable;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAppend;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.random.Random;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
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
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
 * This is done by dividing the sampled rows in {@link Approximate#BUCKET_COUNT}
 * buckets, computing the aggregate functions for each bucket, and using these
 * sampled values to compute confidence intervals.
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

    public interface LogicalPlanRunner {
        void run(LogicalPlan plan, ActionListener<Result> listener);
    }

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
        Rename.class
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
     */
    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_MULTIVALUED_AGGS = Set.of(
        org.elasticsearch.xpack.esql.expression.function.aggregate.Sample.class
    );

    /**
     * These scalar functions produce multivalued output. When they are applied
     * to an approximated field, confidence intervals make no sense anymore and
     * are dropped.
     */
    private static final Set<Class<? extends EsqlScalarFunction>> MULTIVALUED_OUTPUT_FUNCTIONS = Set.of(MvAppend.class);

    // TODO: find a good default value, or alternative ways of setting it
    private static final int SAMPLE_ROW_COUNT = 100000;

    /**
     * The number of buckets to use for computing confidence intervals.
     */
    private static final int BUCKET_COUNT = 16;

    private static final Logger logger = LogManager.getLogger(Approximate.class);

    private final LogicalPlan logicalPlan;
    private final boolean preservesRows;

    public Approximate(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
        this.preservesRows = verifyPlan();
    }

    /**
     * Computes approximate results for the logical plan.
     */
    public void approximate(LogicalPlanRunner runner, ActionListener<Result> listener) {
        runner.run(sourceCountPlan(), sourceCountListener(runner, listener));
    }

    /**
     * Verifies that a plan is suitable for approximation.
     *
     * @return whether the part of the query until the STATS command preserves all rows
     * @throws VerificationException if the plan is not suitable for approximation
     */
    private boolean verifyPlan() throws VerificationException {
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
                throw new VerificationException(
                    List.of(Failure.fail(plan, "query with [" + plan.nodeName().toUpperCase(Locale.ROOT) + "] cannot be approximated"))
                );
            }
        });

        Holder<Boolean> encounteredStats = new Holder<>(false);
        Holder<Boolean> preservesRows = new Holder<>(true);
        logicalPlan.transformUp(plan -> {
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate) {
                    // Verify that the aggregate functions are supported.
                    encounteredStats.set(true);
                    plan.transformExpressionsOnly(AggregateFunction.class, aggFn -> {
                        if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass()) == false
                            && SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
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
                } else if (plan instanceof LeafPlan == false && ROW_PRESERVING_COMMANDS.contains(plan.getClass()) == false) {
                    // Keep track of whether the plan until the STATS preserves all rows.
                    preservesRows.set(false);
                }
            } else {
                // Multiple STATS commands are not supported.
                if (plan instanceof Aggregate) {
                    throw new VerificationException(List.of(Failure.fail(plan, "query with multiple [STATS] cannot be approximated")));
                }
            }
            return plan;
        });

        return preservesRows.get();
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
                plan = new Aggregate(
                    Source.EMPTY,
                    plan,
                    List.of(),
                    List.of(new Alias(Source.EMPTY, "$count", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*"))))
                );
            } else {
                // Strip everything after the leaf.
                plan = plan.children().getFirst();
            }
            return plan;
        });

        sourceCountPlan.setPreOptimized();
        return sourceCountPlan;
    }

    /**
     * Receives the total number of rows, and runs either the
     * {@link Approximate#approximatePlan} or {@link Approximate#countPlan}
     * depending on whether the original query preserves all rows or not.
     */
    private ActionListener<Result> sourceCountListener(LogicalPlanRunner runner, ActionListener<Result> listener) {
        return listener.delegateFailureAndWrap((countListener, countResult) -> {
            long rowCount = rowCount(countResult);
            logger.debug("sourceCountPlan result: {} rows", rowCount);
            double sampleProbability = rowCount <= SAMPLE_ROW_COUNT ? 1.0 : (double) SAMPLE_ROW_COUNT / rowCount;
            if (preservesRows || sampleProbability == 1.0) {
                runner.run(approximatePlan(sampleProbability), listener);
            } else {
                runner.run(countPlan(sampleProbability), countListener(runner, sampleProbability, listener));
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
                // The leaf plan should be appended by a SAMPLE.
                plan = new Sample(Source.EMPTY, Literal.fromDouble(Source.EMPTY, sampleProbability), plan);
            } else if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    // The STATS function should be replaced by a STATS COUNT(*).
                    encounteredStats.set(true);
                    plan = new Aggregate(
                        Source.EMPTY,
                        aggregate.child(),
                        List.of(),
                        List.of(new Alias(Source.EMPTY, "$count", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*"))))
                    );
                }
            } else {
                // Strip everything after the STATS command.
                plan = plan.children().getFirst();
            }
            return plan;
        });

        countPlan.setPreOptimized();
        return countPlan;
    }

    /**
     * Receives the sampled number of rows reaching the STATS function.
     * Runs either the {@link Approximate#approximatePlan} or a next iteration
     * {@link Approximate#countPlan} depending on whether the current count is
     * sufficient.
     */
    private ActionListener<Result> countListener(LogicalPlanRunner runner, double sampleProbability, ActionListener<Result> listener) {
        return listener.delegateFailureAndWrap((countListener, countResult) -> {
            long rowCount = rowCount(countResult);
            logger.debug("countPlan result (p={}): {} rows", sampleProbability, rowCount);
            double newSampleProbability = sampleProbability * SAMPLE_ROW_COUNT / Math.max(1, rowCount);
            if (rowCount <= SAMPLE_ROW_COUNT / 2 && newSampleProbability < 1.0) {
                runner.run(countPlan(newSampleProbability), countListener(runner, newSampleProbability, listener));
            } else {
                runner.run(approximatePlan(newSampleProbability), listener);
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
     *     <li> {@code EVAL} adding a new column with a random bucket ID
     *     <li> {@code STATS} command with:
     *          <ul>
     *              <li> Each aggregate function replaced by a sample-corrected version (if needed)
     *              <li> {@link Approximate#BUCKET_COUNT} additional columns with a sampled values
     *                   for each aggregate function, sample-corrected (if needed)
     *          </ul>
     *     <li> A filter to remove rows with empty buckets
     *     <li> All commands after the {@code STATS} command, modified to also process
     *          the additional bucket columns where possible
     *     <li> {@code EVAL} to compute confidence intervals for all fields with buckets
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
     * is rewritten to:
     * <pre>
     *     {@code
     *         FROM index
     *             | SAMPLE prob
     *             | EVAL x = 2*x
     *             | STATS s = SUM(x)/prob, `s$bucket:0` = SUM(x) / (prob/B)), ..., `s$bucket:B-1` = SUM(x) / (prob/B) BY group
     *             | WHERE `s$bucket:0` IS NOT NULL AND ... AND `s$bucket:B-1` IS NOT NULL
     *             | EVAL t = s*s, `t$bucket:0` = `s$bucket:0`*`s$bucket:0`, ..., `t$bucket:B-1` = `s$bucket:B-1`*`s$bucket:B-1`
     *             | EVAL `CONFIDENCE_INTERVAL(s)` = CONFIDENCE_INTERVAL(s, MV_APPEND(`s$bucket:0`, ... `s$bucket:B-1`)),
     *                    `CONFIDENCE_INTERVAL(t)` = CONFIDENCE_INTERVAL(t, MV_APPEND(`t$bucket:0`, ... `t$bucket:B-1`))
     *             | KEEP s, t, `CONFIDENCE_INTERVAL(s)`, `CONFIDENCE_INTERVAL(t)`
     *     }
     * </pre>
     */
    private LogicalPlan approximatePlan(double sampleProbability) {
        if (sampleProbability >= 1.0) {
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

                Alias bucketIdField = new Alias(
                    Source.EMPTY,
                    "$bucket_id",
                    new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT))
                );

                List<NamedExpression> aggregates = new ArrayList<>();
                // Ensure that all buckets are non-empty, because empty buckets
                // would mess up the confidence interval computation.
                Expression allBucketsNonEmpty = Literal.TRUE;
                for (NamedExpression aggOrKey : aggregate.aggregates()) {
                    if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                        // This is a grouping key, not an aggregate function.
                        aggregates.add(aggOrKey);
                        continue;
                    }

                    // Replace the original aggregation by a sample-corrected one.
                    Alias aggAlias = (Alias) aggOrKey;
                    AggregateFunction aggFn = (AggregateFunction) aggAlias.child();
                    aggregates.add(aggAlias.replaceChild(correctForSampling(aggFn, sampleProbability)));

                    if (SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
                        // For the supported single-valued aggregations, add buckets with sampled
                        // values, that will be used to compute a confidence interval.
                        // For multivalued aggregations, confidence intervals do not make sense.
                        List<Alias> buckets = new ArrayList<>();
                        for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
                            Alias bucket = new Alias(
                                Source.EMPTY,
                                aggOrKey.name() + "$bucket:" + bucketId,
                                correctForSampling(
                                    aggFn.withFilter(
                                        new MvContains(Source.EMPTY, bucketIdField.toAttribute(), Literal.integer(Source.EMPTY, bucketId))
                                    ),
                                    sampleProbability / BUCKET_COUNT
                                )
                            );
                            buckets.add(bucket);
                            aggregates.add(bucket);
                            allBucketsNonEmpty = new And(
                                Source.EMPTY,
                                allBucketsNonEmpty,
                                aggFn instanceof Count
                                    ? new NotEquals(Source.EMPTY, bucket.toAttribute(), Literal.integer(Source.EMPTY, 0))
                                    : new IsNotNull(Source.EMPTY, bucket.toAttribute())
                            );
                        }
                        fieldBuckets.put(aggOrKey.id(), buckets);
                    }
                }

                // Add the bucket ID, do the aggregations (sampled corrected, including the buckets),
                // and filter out rows with empty buckets.
                plan = new Eval(Source.EMPTY, aggregate.child(), List.of(bucketIdField));
                plan = aggregate.with(plan, aggregate.groupings(), aggregates);
                plan = new Filter(Source.EMPTY, plan, allBucketsNonEmpty);

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
                                for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
                                    final int finalBucketId = bucketId;
                                    Expression bucket = field.child()
                                        .transformDown(
                                            e -> e instanceof NamedExpression ne && fieldBuckets.containsKey(ne.id())
                                                ? fieldBuckets.get(ne.id()).get(finalBucketId).toAttribute()
                                                : e
                                        );
                                    buckets.add(new Alias(Source.EMPTY, field.name() + "$bucket:" + bucketId, bucket));
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

        // Compute the confidence interval for all output fields that have buckets.
        List<Alias> confidenceIntervalsAndReliable = new ArrayList<>();
        for (Attribute output : logicalPlan.output()) {
            if (fieldBuckets.containsKey(output.id())) {
                List<Alias> buckets = fieldBuckets.get(output.id());
                // Collect a multivalued expression with all bucket values, and pass that to the
                // confidence interval computation.
                Expression bucketsMv = buckets.getFirst().toAttribute();
                for (int i = 1; i < BUCKET_COUNT; i++) {
                    bucketsMv = new MvAppend(Source.EMPTY, bucketsMv, buckets.get(i).toAttribute());
                }
                confidenceIntervalsAndReliable.add(
                    new Alias(
                        Source.EMPTY,
                        "CONFIDENCE_INTERVAL(" + output.name() + ")",
                        new ConfidenceInterval(Source.EMPTY, output, bucketsMv)
                    )
                );
                confidenceIntervalsAndReliable.add(
                    new Alias(
                        Source.EMPTY,
                        "RELIABLE(" + output.name() + ")",
                        new Reliable(Source.EMPTY, bucketsMv)
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

        return approximatePlan;
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
