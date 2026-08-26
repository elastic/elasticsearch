/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;

/**
 * Generates subplans to obtain a good sample probability for approximation
 * and the final main plan.
 * <p>
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
public class Approximation implements ApproximationDriver {

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
    private final ApproximationVerifier.QueryProperties queryProperties;
    private final int sampleRowCount;
    private final double maxSampleProbability;

    private Double nextSubPlanSampleProbability;
    private int subPlanIterationCount;
    private final SetOnce<Long> sourceRowCount;
    private final SetOnce<Double> minSampleProbability;

    Approximation(LogicalPlan logicalPlan, ApproximationVerifier.QueryProperties queryProperties, ApproximationSettings settings) {
        this.queryProperties = queryProperties;

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
        } else if (queryProperties.hasGrouping()) {
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
     * Returns the first subplan to execute for approximation, or null if the main plan can be executed directly.
     */
    @Override
    public LogicalPlan firstSubPlan() {
        if (sourceRowCount.get() == null) {
            return sourceCountSubPlan();
        } else if (nextSubPlanSampleProbability != null) {
            return countSubPlan(nextSubPlanSampleProbability);
        } else {
            return null;
        }
    }

    @Override
    public LogicalPlan newMainPlan(LogicalPlan mainPlan, Result result) {
        Double p = processResult(rowCount(result));
        if (p == null) {
            return mainPlan;
        }
        return ApproximationPlan.substituteSampleProbability(mainPlan, p);
    }

    /**
     * Processes the subplan results.
     * Returns the sample probability suitable for approximation if possible,
     * or null if more subplans need to be executed to obtain it.
     */
    Double processResult(long rowCount) {
        if (sourceRowCount.get() == null) {
            return processSourceCount(rowCount);
        } else {
            return processCount(rowCount);
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
    private static LogicalPlan getLeftmostLeaf(LogicalPlan plan) {
        while (plan instanceof LeafPlan == false) {
            plan = switch (plan) {
                case UnaryPlan unaryPlan -> unaryPlan.child();
                case Join join -> join.left();
                case Fork fork -> fork.children().getFirst();
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
        } else if (queryProperties.preservesRows()) {
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
                    String aggName = Attribute.rawTemporaryName("count");
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
    Double processCount(long rowCount) {
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
    static long rowCount(Result countResult) {
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
