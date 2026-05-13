/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.ConfidenceInterval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteApproximationPlan;
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
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.RegisteredDomain;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.UserAgent;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class computes approximate results for certain classes of ES|QL queries.
 * Approximate results are usually much faster to compute than exact results.
 * <p>
 * A query is currently suitable for approximation if:
 * <ul>
 *   <li> it contains exactly one {@code STATS} command
 *   <li> the other processing commands are from the supported set
 *        ({@link Approximation#SUPPORTED_COMMANDS}); this set contains almost all
 *        unary commands, and some non-unary ones; most notably not {@code FORK}.
 *   <li> the aggregate functions are from the supported set
 *        ({@link Approximation#SUPPORTED_SINGLE_VALUED_AGGS} and
 *         {@link Approximation#SUPPORTED_MULTIVALUED_AGGS})
 * </ul>
 * Some of these restrictions may be lifted in the future.
 * <p>
 * When these conditions are met, the query is replaced by an approximation query
 * that samples documents before the STATS, and extrapolates the aggregate
 * functions if needed. This new logical plan is generated by
 * {@link ApproximationPlan#get}. The substitution of the original query by the
 * approximation query happens during logical plan optimization, in the rule
 * {@link SubstituteApproximationPlan}.
 * <p>
 * In addition to approximate results, confidence intervals are also computed.
 * This is done by dividing the sampled rows {@link ApproximationPlan#TRIAL_COUNT}
 * times into {@link ApproximationPlan#BUCKET_COUNT} buckets, computing the aggregate
 * functions for each bucket, and using these sampled values to compute
 * confidence intervals with the bias-corrected and accelerated (BCa) bootstrap
 * method, see also {@link ConfidenceInterval}.
 * <p>
 * The initial approximation plan contains a placeholder for the sample probability,
 * which is determined during subplan execution, and is based on results set size.
 * To obtain an appropriate sample probability, first a target number of rows
 * is set. This is determined via the {@link ApproximationSettings}.
 * Next, the total number of rows in the source index is counted via the subplan
 * {@link Approximation#sourceCountSubPlan}. This plan always executes fast. When
 * there are no commands that can change the number of rows, the sample
 * probability can be directly computed as a ratio of the target number of rows
 * and this total number.
 * <p>
 * In the presence of commands that can change the number of rows (e.g. filtering),
 * another step is needed. The first goal is to find a sample probability that
 * leads to approximately {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} rows,
 * and when this probability is found, a sample probability leading to the target
 * number of rows is computed.
 * <p>
 * This is done by setting the initial sample probability to the ratio of
 * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} and the total number
 * of rows in the source index, and a number of rows is sampled with the subplan
 * {@link Approximation#countSubPlan}. As long as the sampled number of rows is too
 * small, the probability is increased until a good probability is reached. This
 * final probability is used to compute the probability using for approximating
 * the original query.
 */
public class Approximation {

    public record QueryProperties(boolean hasGrouping, boolean canDecreaseRowCount) {}

    /**
     * These processing commands are fully supported.
     * <p>
     * When a command is not supported, it should be added to
     * ApproximationSupportTests.UNSUPPORTED_COMMANDS
     * to make sure all commands are captured.
     */
    static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS;
    static {
        Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS_BUILDER = new HashSet<>(
            List.of(
                Aggregate.class,
                Completion.class,
                Dissect.class,
                Enrich.class,
                EsRelation.class,
                Eval.class,
                Filter.class,
                Grok.class,
                Insist.class,
                LocalRelation.class,
                MvExpand.class,
                OrderBy.class,
                Project.class,
                RegexExtract.class,
                RegisteredDomain.class,
                Rerank.class,
                Row.class,
                Sample.class,
                SampledAggregate.class,
                UriParts.class,
                UserAgent.class
            )
        );
        if (EsqlCapabilities.Cap.APPROXIMATION_LOOKUP_JOIN.isEnabled()) {
            SUPPORTED_COMMANDS_BUILDER.add(Join.class);
        }
        if (EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled()) {
            SUPPORTED_COMMANDS_BUILDER.add(InlineJoin.class);
            SUPPORTED_COMMANDS_BUILDER.add(StubRelation.class);  // temporary node
        }
        SUPPORTED_COMMANDS = Collections.unmodifiableSet(SUPPORTED_COMMANDS_BUILDER);
    }

    /**
     * These processing commands are only supported after the initial STATS.
     */
    static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS_AFTER_STATS = Set.of(
        // It makes no sense to approximate "FROM index | LIMIT N | STATS ...".
        // Furthermore, the LIMIT here breaks the estimation of the sample probability.
        Limit.class,
        // Same for LIMIT BY, SORT, or SORT + LIMIT BY
        LimitBy.class,
        TopN.class,
        TopNBy.class,
        // CHANGE_POINT implicitly uses LIMIT
        ChangePoint.class
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
        CountApproximate.class,
        Median.class,
        MedianAbsoluteDeviation.class,
        Percentile.class,
        StdDev.class,
        Sum.class,
        Variance.class,
        WeightedAvg.class
    );

    /**
     * These multivalued aggregate functions work well with random sampling.
     * However, confidence intervals make no sense anymore and are dropped.
     */
    static final Set<Class<? extends AggregateFunction>> SUPPORTED_MULTIVALUED_AGGS = Set.of(
        org.elasticsearch.xpack.esql.expression.function.aggregate.Sample.class
    );

    /**
     * The target number of sampled rows to estimate the total number of rows
     * reaching the STATS function. This is only relevant when there are commands
     * that can change the number of rows. This value leads to an error of about
     * 1% in the estimated count.
     */
    private static final int ROW_COUNT_FOR_COUNT_ESTIMATION = 10_000;

    // TODO: finetune these query approximation parameters:
    //
    // The sample probability threshold should depend on the aggregation
    // functions. For trivial functions like COUNT and SUM, the threshold should
    // be lower than for computationally heavier ones, like MEDIAN and PERCENTILE.
    // It may also depend on the presence of grouping, on whether the grouping
    // is sparse or dense, and the data size (many/few large/small keys) on the
    // coordinator.
    //
    // The default row counts should probably scale with cluster size. Otherwise,
    // as the cluster size increases, fewer and fewer rows per node are sampled.
    // This leads to much overhead per sampled rows, making the system inefficient.
    // If cluster size is hard to get, index size might be a good proxy.
    //
    // See also: https://github.com/elastic/elasticsearch/issues/144590

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
     * Don't sample with a probability higher than this threshold. The cost of
     * tracking confidence intervals doesn't outweigh the benefits of sampling.
     * This threshold only applies when calculating confidence intervals. When
     * they are disabled (by setting "confidence_level":null), any sample
     * probability is allowed,
     */
    private static final double SAMPLE_PROBABILITY_THRESHOLD = 0.10;

    private static final Logger logger = LogManager.getLogger(Approximation.class);

    private static final Expression WILDCARD = Literal.keyword(Source.EMPTY, StringUtils.WILDCARD);
    private static final AggregateFunction COUNT_ALL_ROWS_EXACT = new Count(Source.EMPTY, WILDCARD);
    private static final AggregateFunction COUNT_ALL_ROWS_APPROXIMATE = new CountApproximate(Source.EMPTY, WILDCARD);

    private final LogicalPlan logicalPlan;
    private final QueryProperties queryProperties;
    private final int sampleRowCount;
    private final double maxSampleProbability;

    private Double nextSubPlanSampleProbability;
    private int subPlanIterationCount;
    private final SetOnce<Long> sourceRowCount;
    private final SetOnce<Double> minSampleProbability;

    public Approximation(LogicalPlan logicalPlan, ApproximationSettings settings) {
        this.queryProperties = verifyPlanOrThrow(logicalPlan);
        // The plan is executed multiple times. Use CopyingLocalSupplier to
        // make sure the page is not released between executions.
        this.logicalPlan = logicalPlan.transformUp(LocalRelation.class, lr -> {
            if (lr.supplier() instanceof CopyingLocalSupplier == false) {
                return new LocalRelation(lr.source(), lr.output(), new CopyingLocalSupplier(lr.supplier().get()));
            }
            return lr;
        });

        if (settings.rows() != null) {
            sampleRowCount = settings.rows();
        } else if (queryProperties.hasGrouping) {
            sampleRowCount = DEFAULT_ROW_COUNT_WITH_GROUPING;
        } else {
            sampleRowCount = DEFAULT_ROW_COUNT_WITHOUT_GROUPING;
        }
        maxSampleProbability = settings.confidenceLevel() == null ? 1.0 : SAMPLE_PROBABILITY_THRESHOLD;

        nextSubPlanSampleProbability = null;
        subPlanIterationCount = 0;
        sourceRowCount = new SetOnce<>();
        minSampleProbability = new SetOnce<>();
    }

    /**
     * Verifies that a plan is suitable for approximation.
     * @return the query properties relevant for approximation if it's suitable, or null otherwise
     * Adds warning headers as a side effect when the plan is not suitable
     */
    public static QueryProperties verifyPlan(LogicalPlan logicalPlan) {
        try {
            return verifyPlanOrThrow(logicalPlan);
        } catch (VerificationException e) {
            HeaderWarning.addWarning(e.getMessage());
            return null;
        }
    }

    static QueryProperties verifyPlanOrThrow(LogicalPlan logicalPlan) {
        // The plan must contain a STATS command.
        if (logicalPlan.anyMatch(plan -> plan instanceof Aggregate) == false) {
            Location location = logicalPlan.collectLeaves().getFirst().source().source();
            throw new VerificationException(
                "line {}:{}: approximation not supported: query must have [STATS] with aggregation function(s) that can be approximated",
                location.getLineNumber(),
                location.getColumnNumber()
            );
        }
        // Verify that all commands are supported.
        logicalPlan.forEachUp(plan -> {
            if ((SUPPORTED_COMMANDS.contains(plan.getClass()) == false && SUPPORTED_COMMANDS_AFTER_STATS.contains(plan.getClass()) == false)
                || (plan instanceof EsRelation esRelation && SUPPORTED_INDEX_MODES.contains(esRelation.indexMode()) == false)) {
                // TODO: ideally just return the command from the source
                throw new VerificationException(
                    "line {}:{}: approximation not supported: query with [{}] cannot be approximated",
                    plan.source().source().getLineNumber(),
                    plan.source().source().getColumnNumber(),
                    plan.sourceText()
                );
            }
        });

        Holder<Boolean> encounteredStats = new Holder<>(false);
        Holder<Boolean> hasGrouping = new Holder<>();
        Holder<Boolean> canDecreaseRowCount = new Holder<>(false);

        logicalPlan.forEachUp(plan -> {
            if (encounteredStats.get() == false) {
                if (SUPPORTED_COMMANDS_AFTER_STATS.contains(plan.getClass())) {
                    throw new VerificationException(
                        "line {}:{}: approximation not supported: query with [{}] before [STATS] cannot be approximated",
                        plan.source().source().getLineNumber(),
                        plan.source().source().getColumnNumber(),
                        plan.sourceText()
                    );
                }
                if (plan instanceof Aggregate aggregate) {
                    // Verify that the aggregate functions are supported.
                    encounteredStats.set(true);
                    hasGrouping.set(aggregate.groupings().isEmpty() == false);
                    plan.forEachExpression(AggregateFunction.class, aggFn -> {
                        if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass()) == false
                            && SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
                            // TODO: ideally just return aggregate function from the source
                            throw new VerificationException(
                                "line {}:{}: approximation not supported: aggregation function [{}] cannot be approximated",
                                aggFn.source().source().getLineNumber(),
                                aggFn.source().source().getColumnNumber(),
                                aggFn.sourceText()
                            );
                        }
                        if (aggFn.dataType().isNumeric() == false) {
                            throw new VerificationException(
                                "line {}:{}: approximation not supported: aggregation function [{}] must return a numeric value; got [{}]",
                                aggFn.source().source().getLineNumber(),
                                aggFn.source().source().getColumnNumber(),
                                aggFn.sourceText(),
                                aggFn.dataType()
                            );
                        }
                    });
                } else if (plan instanceof LeafPlan == false) {
                    if (ROW_NON_DECREASING_COMMANDS.contains(plan.getClass()) == false) {
                        canDecreaseRowCount.set(true);
                    }
                }
            } else {
                // Multiple STATS commands are not supported.
                if (plan instanceof Aggregate) {
                    throw new VerificationException(
                        "line {}:{}: approximation not supported: query with multiple [STATS] cannot be approximated",
                        plan.source().source().getLineNumber(),
                        plan.source().source().getColumnNumber()
                    );
                }
            }
        });

        return new QueryProperties(hasGrouping.get(), canDecreaseRowCount.get());
    }

    /**
     * Returns the first subplan to execute for approximation, or null if the main plan can be executed directly.
     */
    public LogicalPlan firstSubPlan() {
        if (sourceRowCount.get() == null) {
            return sourceCountSubPlan();
        } else if (nextSubPlanSampleProbability != null) {
            return countSubPlan(nextSubPlanSampleProbability);
        } else {
            return null;
        }
    }

    /**
     * Processes the subplan results.
     * Returns the sample probability suitable for approximation if possible,
     * or null if more subplans need to be executed to obtain it.
     */
    public Double processResult(Result result) {
        if (sourceRowCount.get() == null) {
            return processSourceCount(rowCount(result));
        } else {
            return processCount(rowCount(result));
        }
    }

    /**
     * Plan that counts the number of rows in the source index.
     * This is the ES|QL query:
     * <pre>
     * {@code FROM index | STATS COUNT(*)}
     * </pre>
     */
    private LogicalPlan sourceCountSubPlan() {
        LogicalPlan leaf = getLeftmostLeaf(logicalPlan);
        LogicalPlan sourceCountPlan = new Aggregate(
            Source.EMPTY,
            leaf,
            List.of(),
            List.of(new Alias(Source.EMPTY, Attribute.rawTemporaryName("source_count"), COUNT_ALL_ROWS_EXACT))
        );
        sourceCountPlan.setOptimized();
        return sourceCountPlan;
    }

    /**
     * Returns the leftmost leaf of a plan, which is the large source index for approximation.
     */
    private LogicalPlan getLeftmostLeaf(LogicalPlan plan) {
        while (plan instanceof LeafPlan == false) {
            plan = switch (plan) {
                case UnaryPlan unaryPlan -> unaryPlan.child();
                case Join join -> join.left();
                default -> throw new IllegalStateException("unsupported plan type: " + plan.getClass());
            };
        }
        return plan;
    }

    /**
     * Determines either the final sample probability or whether more subplans
     * need to the executed, based on the total number of rows in the source
     * index and the query properties.
     */
    private Double processSourceCount(long sourceRowCount) {
        logger.debug("total number of source rows: [{}] rows", sourceRowCount);
        this.sourceRowCount.set(sourceRowCount);
        // At least `sampleRowCount` source rows must be sampled.
        // When there are too few, process all of them without sampling.
        if (sourceRowCount <= sampleRowCount) {
            // If there are few source rows, run the original query.
            nextSubPlanSampleProbability = null;
            return 1.0;
        }
        double sampleProbability = (double) sampleRowCount / sourceRowCount;
        minSampleProbability.set(sampleProbability);

        if (sampleProbability >= maxSampleProbability) {
            // If the sample probability is large, we can directly run the original query without sampling.
            logger.debug("using original plan (too few source rows)");
            nextSubPlanSampleProbability = null;
            return 1.0;
        } else if (queryProperties.canDecreaseRowCount == false) {
            // If the query preserves all rows, we can directly approximate with the sample probability.
            nextSubPlanSampleProbability = null;
            return sampleProbability;
        } else {
            // Otherwise, we need to sample the number of rows first to obtain a good sample probability.
            nextSubPlanSampleProbability = Math.min(1.0, (double) ROW_COUNT_FOR_COUNT_ESTIMATION / sourceRowCount);
            return null;
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
    private LogicalPlan countSubPlan(double sampleProbability) {
        Holder<Boolean> encounteredStats = new Holder<>(false);
        LogicalPlan countPlan = logicalPlan.transformUp(plan -> {
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate aggregate) {
                    // The STATS function should be replaced by a STATS COUNT(*).
                    encounteredStats.set(true);
                    String aggName = Attribute.rawTemporaryName("count", Double.toString(sampleProbability));
                    if (sampleProbability == 1.0) {
                        List<NamedExpression> aggregations = List.of(new Alias(Source.EMPTY, aggName, COUNT_ALL_ROWS_EXACT));
                        plan = new Aggregate(Source.EMPTY, aggregate.child(), List.of(), aggregations);
                    } else {
                        List<NamedExpression> aggregations = List.of(new Alias(Source.EMPTY, aggName, COUNT_ALL_ROWS_APPROXIMATE));
                        plan = new SampledAggregate(
                            Source.EMPTY,
                            aggregate.child(),
                            List.of(),
                            aggregations,
                            aggregations,
                            Literal.fromDouble(Source.EMPTY, sampleProbability)
                        );
                    }
                }
            } else if (plan instanceof LeafPlan == false) {
                // Strip everything after the STATS command.
                plan = plan.children().getFirst();
            }
            return plan;
        });
        countPlan.setOptimized();
        return countPlan;
    }

    /**
     * Receives the sampled number of rows reaching the STATS function and determines
     * whether the next iteration should run the main approximation plan (if a good
     * sample probability is found), or another {@link Approximation#countSubPlan}
     * with higher sample probability,
     * <p>
     * The first iteration uses a probability of
     * <pre>
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} / {@link Approximation#sourceRowCount}
     * </pre>
     * which is set by the {@link Approximation#processSourceCount}.
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
     * The iteration stops when a rowCount larger than {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} / 2
     * is encountered. The division by 2 is to stop when the random sampling
     * returns a few less rows than aimed for.
     * <p>
     * For zero rowCount the sample probability is multiplied by
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION}. That means the next
     * iteration can sample a number of rows between 0 and the target count,
     * but can never overshoot by much.
     * <p>
     * Regarding the iteration count: in the worst case (very large index, very few
     * matching docs), the first few iterations return zero rows, and the sample probability
     * is multiplied by {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} each time.
     * Next, there's one iteration that returns a positive small row count, and the
     * probability is scaled up the correct value. Next, there's one final iteration
     * with approximately {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} rows.
     * <p>
     * This result in a maximum iteration count of about:
     * <pre>
     * log(sourceRowCount, {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION}) + 2
     * </pre>
     * which is at most 7 when sourceRowCount=MAX_LONG (huge!) and
     * {@link Approximation#ROW_COUNT_FOR_COUNT_ESTIMATION} = 10000.
     * To be safe, the maximum iteration count is capped at 10, and an exception is thrown
     * when this count is exceeded.
     */
    private Double processCount(long rowCount) {
        subPlanIterationCount += 1;
        if (subPlanIterationCount > 10) {
            throw new IllegalStateException("Approximation count iteration limit exceeded");
        }
        double sampleProbability = nextSubPlanSampleProbability;
        // The row count is sample-corrected, however here we want the actual
        // (not-corrected) number of rows reaching the STATS.
        rowCount = Math.round(sampleProbability * rowCount);
        logger.debug("estimated number of rows reaching STATS (p=[{}]): [{}] rows", sampleProbability, rowCount);
        double newSampleProbability = Math.min(1.0, sampleProbability * sampleRowCount / Math.max(1, rowCount));
        newSampleProbability = Math.max(newSampleProbability, minSampleProbability.get());
        if (newSampleProbability >= maxSampleProbability) {
            // If the new sample probability is large, run the original query.
            logger.debug("using original plan (too few rows)");
            nextSubPlanSampleProbability = null;
            return 1.0;
        } else if (rowCount <= ROW_COUNT_FOR_COUNT_ESTIMATION / 2) {
            // Not enough rows are sampled yet; increase the sample probability and try again.
            nextSubPlanSampleProbability = Math.min(1.0, sampleProbability * ROW_COUNT_FOR_COUNT_ESTIMATION / Math.max(1, rowCount));
            return null;
        } else {
            // A good sample probability is found; run the approximation plan.
            nextSubPlanSampleProbability = null;
            return newSampleProbability;
        }
    }

    /**
     * Returns the row count in the result and closes the result.
     */
    private long rowCount(Result countResult) {
        assert countResult.pages().size() == 1;
        assert countResult.pages().getFirst().getBlockCount() == 1;
        assert countResult.pages().getFirst().getPositionCount() == 1;

        long rowCount = switch (countResult.pages().getFirst().getBlock(0)) {
            case DoubleBlock doubleBlock -> Math.round(doubleBlock.getDouble(0));
            case LongBlock longBlock -> longBlock.getLong(0);
            default -> throw new IllegalStateException("Unexpected value: " + countResult.pages().getFirst().getBlock(0));
        };
        countResult.pages().getFirst().close();
        return rowCount;
    }
}
