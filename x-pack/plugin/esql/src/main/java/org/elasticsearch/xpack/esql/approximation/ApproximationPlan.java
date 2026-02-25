/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.LeafExpression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.ConfidenceInterval;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAppend;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteApproximationPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The approximation plan, that is substituted during logical plan optimization
 * in the rule {@link SubstituteApproximationPlan}.
 * <p>
 * See the JavaDocs of {@link Approximation} for more details.
 */
public class ApproximationPlan {

    /**
     * The column name for the bucket ID in the sampled aggregate. This is used
     * to assign each sampled row to a bucket, to compute confidence intervals.
     */
    public static final String BUCKET_ID_COLUMN_NAME = "$bucket_id";

    /**
     * The number of times (trials) the sampled rows are divided into buckets.
     */
    static final int TRIAL_COUNT = 2;

    /**
     * The number of buckets to use for computing confidence intervals.
     */
    static final int BUCKET_COUNT = 15;

    /**
     * Default confidence level for confidence intervals.
     */
    static final double DEFAULT_CONFIDENCE_LEVEL = 0.90;

    /**
     * For grouped statistics (STATS ... BY), a grouping needs at least this
     * number of sampled rows to be included in the results. For a simple
     * aggregation as count, this still leads to acceptable confidence
     * intervals. When the number of rows is closed to the limit, a result
     * will never be marked as "certified reliable", because all buckets need
     * to data for that.
     */
    static final int MIN_ROW_COUNT_FOR_RESULT_INCLUSION = 10;

    /**
     * These aggregate functions need to be corrected for random sampling, by
     * scaling up the sampled value by the inverse of the sampling probability.
     * Other aggregate functions do not need any correction.
     */
    private static final Set<Class<? extends AggregateFunction>> SAMPLE_CORRECTED_AGGS = Set.of(Count.class, Sum.class);

    /**
     * These numerical scalar functions produce multivalued output. This means that
     * confidence intervals cannot be computed anymore and are dropped.
     * <p>
     * Numerical scalar functions that produce multivalued output should be added
     * here. Forgetting to do so leads to confidence intervals columns for the
     * multivalued fields, that are filled with nulls.
     */
    private static final Set<Class<? extends EsqlScalarFunction>> MULTIVALUED_OUTPUT_FUNCTIONS = Set.of(MvAppend.class);

    private static final AggregateFunction COUNT_ALL_ROWS = new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, StringUtils.WILDCARD));

    /**
     * A placeholder expression in the main approximation plan, that is replaced
     * by the actual value after subplan execution.
     */
    public static class SampleProbabilityPlaceHolder extends LeafExpression {

        public SampleProbabilityPlaceHolder(Source source) {
            super(source);
        }

        @Override
        public Nullability nullable() {
            return Nullability.TRUE;
        }

        @Override
        public DataType dataType() {
            return DataType.DOUBLE;
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this);
        }

        @Override
        public String toString() {
            return "SampleProbabilityPlaceHolder";
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && getClass() == obj.getClass();
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private static final Logger logger = LogManager.getLogger(ApproximationPlan.class);

    /**
     * Returns a plan that approximates the original plan and computes confidence intervals.
     * This approximation query consists of the following:
     * <ul>
     *     <li> Source command
     *     <li> {@code SAMPLE} with a {@link ApproximationPlan.SampleProbabilityPlaceHolder} for the sample probability
     *     <li> All commands before the {@code STATS} command
     *     <li> {@code EVAL} adding a new column with random bucket IDs for each trial
     *     <li> {@code STATS} command with:
     *          <ul>
     *              <li> {@code COUNT} to track the sample size
     *              <li> Each aggregate function replaced by a sample-corrected version (if needed)
     *              <li> {@link ApproximationPlan#TRIAL_COUNT} * {@link ApproximationPlan#BUCKET_COUNT} additional columns
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
     *             | SAMPLE prob = SampleProbabilityPlaceHolder
     *             | EVAL x = 2*x
     *             | EVAL bucketId = MV_APPEND(RANDOM(B), ... , RANDOM(B))  // T times
     *             | STATS sampleSize = COUNT(*),
     *                     s = SUM(x) / prob,
     *                     `s$0` = SUM(x) / (prob/B)) WHERE MV_SLICE(bucketId, 0, 0) == 0
     *                     ...,
     *                     `s$T*B-1` = SUM(x) / (prob/B) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *               BY group
     *             | WHERE sampleSize >= sampleSizeThreshold
     *             | EVAL t = s*s, `t$0` = `s$0`*`s$0`, ..., `t$T*B-1` = `s$T*B-1`*`s$T*B-1`
     *             | EVAL `CONFIDENCE_INTERVAL(s)` = CONFIDENCE_INTERVAL(s, MV_APPEND(`s$0`, ... `s$T*B-1`), T, B, 0.90),
     *                    `CONFIDENCE_INTERVAL(t)` = CONFIDENCE_INTERVAL(t, MV_APPEND(`t$0`, ... `t$T*B-1`), T, B, 0.90)
     *             | KEEP s, t, `CONFIDENCE_INTERVAL(s)`, `CONFIDENCE_INTERVAL(t)`
     *     }
     * </pre>
     */
    public static LogicalPlan get(LogicalPlan logicalPlan, ApproximationSettings settings) {
        logger.debug("generating approximation plan");

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
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate == false) {
                    // Commands before the first STATS function should be left unchanged.
                    return plan;
                } else {
                    // The first STATS function should be replaced by a sample-corrected STATS
                    // and buckets (for computing confidence intervals).
                    encounteredStats.set(true);
                    return sampleCorrectedAggregateAndBuckets((Aggregate) plan, fieldBuckets, uncorrectedExpressions);
                }
            } else {
                // After the STATS function, any processing of fields that have buckets, should
                // also process the buckets, so that confidence intervals for the dependent fields
                // can be computed.
                return planIncludingBuckets(plan, fieldBuckets, uncorrectedExpressions);
            }
        });

        // Add the confidence intervals for all fields with buckets.
        double confidenceLevel = settings.confidenceLevel() != null ? settings.confidenceLevel() : DEFAULT_CONFIDENCE_LEVEL;
        approximationPlan = new Eval(Source.EMPTY, approximationPlan, getConfidenceIntervals(logicalPlan, fieldBuckets, confidenceLevel));

        // Drop all bucket fields and uncorrected fields from the output.
        Set<Attribute> dropAttributes = Stream.concat(
            fieldBuckets.values().stream().flatMap(List::stream),
            uncorrectedExpressions.values().stream()
        ).map(NamedExpression::toAttribute).collect(Collectors.toSet());

        List<Attribute> keepAttributes = new ArrayList<>(approximationPlan.output());
        keepAttributes.removeAll(dropAttributes);
        approximationPlan = new Project(Source.EMPTY, approximationPlan, keepAttributes);
        logger.debug("approximation plan:\n{}", approximationPlan);
        return approximationPlan;
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
    private static LogicalPlan sampleCorrectedAggregateAndBuckets(
        Aggregate aggregate,
        Map<NameId, List<Alias>> fieldBuckets,
        Map<NameId, NamedExpression> uncorrectedExpressions
    ) {
        Expression sampleProbability = new SampleProbabilityPlaceHolder(Source.EMPTY);
        Expression bucketSampleProbability = new Div(Source.EMPTY, sampleProbability, Literal.integer(Source.EMPTY, BUCKET_COUNT));

        Expression randomBucketId = new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT));
        Expression bucketIds = randomBucketId;
        for (int trialId = 1; trialId < TRIAL_COUNT; trialId++) {
            bucketIds = new MvAppend(Source.EMPTY, bucketIds, randomBucketId);
        }
        // TODO: use theoretically non-conflicting names.
        Alias bucketIdField = new Alias(Source.EMPTY, BUCKET_ID_COLUMN_NAME, bucketIds);

        // The aggregate functions in the approximation plan.
        List<NamedExpression> bucketAggregates = new ArrayList<>();

        // List of expressions that must be evaluated after the sampled aggregation.
        // These consist of:
        // - sample corrections (to correct counts/sums for sampling)
        // - replace zero counts by NULLs (for confidence interval computation)
        // - exact total row count if COUNT(*) is used (to avoid sampling errors there)
        List<Alias> evals = new ArrayList<>();
        List<NamedExpression> originalAggregates = new ArrayList<>();
        List<Attribute> projections = new ArrayList<>();

        for (NamedExpression aggOrKey : aggregate.aggregates()) {
            if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                // This is a grouping key, not an aggregate function.
                originalAggregates.add(aggOrKey);
                projections.add(aggOrKey.toAttribute());
                continue;
            }

            Alias aggAlias = (Alias) aggOrKey;
            AggregateFunction aggFn = (AggregateFunction) aggAlias.child();

            if (Approximation.SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass())) {
                // For the supported single-valued aggregations, add buckets with sampled
                // values, that will be used to compute a confidence interval.
                // For multivalued aggregations, confidence intervals do not make sense.
                List<Alias> buckets = new ArrayList<>();
                for (int trialId = 0; trialId < TRIAL_COUNT; trialId++) {
                    for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
                        Expression bucketIdFilter = new Equals(
                            Source.EMPTY,
                            new MvSlice(
                                Source.EMPTY,
                                bucketIdField.toAttribute(),
                                Literal.integer(Source.EMPTY, trialId),
                                Literal.integer(Source.EMPTY, trialId)
                            ),
                            Literal.integer(Source.EMPTY, bucketId)
                        );
                        Expression bucket = aggFn.withFilter(
                            aggFn.hasFilter() == false ? bucketIdFilter : new And(Source.EMPTY, aggFn.filter(), bucketIdFilter)
                        );
                        Alias bucketAlias = new Alias(
                            Source.EMPTY,
                            aggOrKey.name() + "$bucket$" + (trialId * BUCKET_COUNT + bucketId),
                            bucket
                        );
                        if (SAMPLE_CORRECTED_AGGS.contains(aggFn.getClass()) == false) {
                            buckets.add(bucketAlias);
                            bucketAggregates.add(bucketAlias);
                            projections.add(bucketAlias.toAttribute());
                        } else {
                            Alias uncorrectedBucketAlias = new Alias(Source.EMPTY, bucketAlias.name() + "$uncorrected", bucket);
                            uncorrectedExpressions.put(bucketAlias.id(), uncorrectedBucketAlias);
                            bucketAggregates.add(uncorrectedBucketAlias);
                            projections.add(uncorrectedBucketAlias.toAttribute());

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

                            Expression correctedBucket = correctForSampling(uncorrectedBucket, bucketSampleProbability, null);
                            Alias correctedBucketAlias = bucketAlias.replaceChild(correctedBucket);
                            evals.add(correctedBucketAlias);
                            projections.add(correctedBucketAlias.toAttribute());
                            buckets.add(correctedBucketAlias);
                        }
                    }
                }
                fieldBuckets.put(aggOrKey.id(), buckets);
            }

            // Replace the original aggregation by a sample-corrected one if needed.
            if (SAMPLE_CORRECTED_AGGS.contains(aggFn.getClass()) == false) {
                originalAggregates.add(aggAlias);
                projections.add(aggAlias.toAttribute());
            } else {
                Alias uncorrectedAggAlias = new Alias(aggAlias.source(), aggAlias.name() + "$uncorrected", aggFn);
                uncorrectedExpressions.put(aggAlias.id(), uncorrectedAggAlias);
                originalAggregates.add(uncorrectedAggAlias);
                projections.add(uncorrectedAggAlias.toAttribute());

                Expression correctedAgg = correctForSampling(
                    uncorrectedAggAlias.toAttribute(),
                    sampleProbability,
                    fieldBuckets.get(aggOrKey.id())
                );
                evals.add(aggAlias.replaceChild(correctedAgg));
                projections.add(aggAlias.toAttribute());
            }
        }

        List<NamedExpression> aggregates = Stream.concat(originalAggregates.stream(), bucketAggregates.stream())
            .collect(Collectors.toList());

        Alias sampleSize = null;
        if (aggregate.groupings().isEmpty() == false) {
            // Add the sample size per grouping to filter out groups with too few sampled rows.
            sampleSize = new Alias(Source.EMPTY, "$sample_size", COUNT_ALL_ROWS);
            aggregates.add(sampleSize);
            originalAggregates.add(sampleSize);
        }

        // Add the bucket ID, do the aggregations (sampled corrected, including the buckets),
        // and filter out rows with too few sampled values.
        LogicalPlan plan = new Eval(Source.EMPTY, aggregate.child(), List.of(bucketIdField));

        plan = new SampledAggregate(aggregate.source(), plan, aggregate.groupings(), aggregates, originalAggregates, sampleProbability);

        if (sampleSize != null) {
            List<Attribute> allBuckets = Expressions.asAttributes(bucketAggregates);
            plan = new Filter(
                Source.EMPTY,
                plan,
                new Or(
                    Source.EMPTY,
                    new IsNull(Source.EMPTY, new Coalesce(Source.EMPTY, allBuckets.getFirst(), allBuckets.subList(1, allBuckets.size()))),
                    new GreaterThanOrEqual(
                        Source.EMPTY,
                        sampleSize.toAttribute(),
                        Literal.integer(Source.EMPTY, MIN_ROW_COUNT_FOR_RESULT_INCLUSION)
                    )
                )
            );
        }

        plan = new Eval(Source.EMPTY, plan, evals);
        return new Project(Source.EMPTY, plan, projections);
    }

    /**
     * Corrects an aggregation function for random sampling.
     * Some functions (like COUNT and SUM) need to be scaled up by the inverse of
     * the sampling probability, while others (like AVG and MEDIAN) do not.
     */
    private static Expression correctForSampling(Expression expr, Expression sampleProbability, List<Alias> buckets) {
        Expression correctedAgg = new Div(expr.source(), expr, sampleProbability);
        correctedAgg = switch (expr.dataType()) {
            case DOUBLE -> correctedAgg;
            case LONG -> new ToLong(expr.source(), correctedAgg);
            default -> throw new IllegalStateException("unexpected data type [" + expr.dataType() + "]");
        };
        if (buckets != null) {
            // All buckets being null indicates that the query was executed
            // exactly, hence no sampling correction must be applied.
            List<Expression> rest = buckets.subList(1, buckets.size()).stream().map(Alias::toAttribute).collect(Collectors.toList());
            correctedAgg = new Case(
                Source.EMPTY,
                new IsNull(Source.EMPTY, new Coalesce(Source.EMPTY, buckets.getFirst().toAttribute(), rest)),
                List.of(expr, correctedAgg)
            );
        }
        return correctedAgg;
    }

    /**
     * Returns a plan that also processes the buckets for fields that have them.
     * Luckily, there's only a limited set of commands that have to do something
     * with the buckets: EVAL, PROJECT and MV_EXPAND.
     */
    private static LogicalPlan planIncludingBuckets(
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
    private static LogicalPlan evalIncludingBuckets(
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
    private static LogicalPlan projectIncludingBuckets(Project project, Map<NameId, List<Alias>> fieldBuckets) {
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
    private static LogicalPlan mvExpandIncludingBuckets(MvExpand mvExpand, Map<NameId, List<Alias>> fieldBuckets) {
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
    private static List<Alias> getConfidenceIntervals(
        LogicalPlan logicalPlan,
        Map<NameId, List<Alias>> fieldBuckets,
        double confidenceLevel
    ) {
        Expression constNaN = new Literal(Source.EMPTY, Double.NaN, DataType.DOUBLE);
        Expression trialCount = Literal.integer(Source.EMPTY, TRIAL_COUNT);
        Expression bucketCount = Literal.integer(Source.EMPTY, BUCKET_COUNT);
        Expression confidenceLevelExpr = Literal.fromDouble(Source.EMPTY, confidenceLevel);

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
                    confidenceLevelExpr
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

    /**
     * Substitutes the {@link SampleProbabilityPlaceHolder} in the approximation plan
     * by the actual sample probability. If the sample probability is 1.0, the
     * SampledAggregate is also replaced by a regular Aggregate.
     */
    public static LogicalPlan substituteSampleProbability(LogicalPlan logicalPlan, double sampleProbability) {
        logicalPlan = logicalPlan.transformExpressionsDown(
            ApproximationPlan.SampleProbabilityPlaceHolder.class,
            prob -> Literal.fromDouble(Source.EMPTY, sampleProbability)
        );
        if (sampleProbability == 1.0) {
            logicalPlan = logicalPlan.transformDown(SampledAggregate.class, agg -> {
                List<Alias> nullBuckets = new ArrayList<>();
                Set<String> originalAggs = agg.originalAggregates().stream().map(NamedExpression::name).collect(Collectors.toSet());
                for (Attribute attr : agg.outputSet()) {
                    if (originalAggs.contains(attr.name()) == false) {
                        nullBuckets.add(new Alias(Source.EMPTY, attr.name(), Literal.NULL, attr.id()));
                    }
                }
                LogicalPlan plan = new Aggregate(agg.source(), agg.child(), agg.groupings(), agg.originalAggregates());
                // All buckets being NULL indicates that the query was executed exactly,
                // leading to trivial confidence intervals.
                plan = new Eval(Source.EMPTY, plan, nullBuckets);
                return plan;
            });
        }
        logicalPlan.setOptimized();
        return logicalPlan;
    }
}
