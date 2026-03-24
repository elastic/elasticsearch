/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
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
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    public static final String BUCKET_ID_COLUMN_NAME = Attribute.rawTemporaryName("approximation", "bucket_id");

    /**
     * Prefix for confidence interval column names in the approximation output.
     */
    public static final String CONFIDENCE_INTERVAL_COLUMN_PREFIX = "_approximation_confidence_interval(";

    /**
     * Prefix for certified column names in the approximation output.
     */
    public static final String CERTIFIED_COLUMN_PREFIX = "_approximation_certified(";

    /**
     * Returns the {@code _meta} map for an approximation column, or {@code null}
     * if the column name does not match an approximation pattern.
     */
    public static Map<String, Object> columnMetadata(Attribute column) {
        if (column.synthetic() == false) {
            return null;
        }
        String columnName = column.name();
        if (columnName.startsWith(CONFIDENCE_INTERVAL_COLUMN_PREFIX) && columnName.endsWith(")")) {
            String sourceColumn = columnName.substring(CONFIDENCE_INTERVAL_COLUMN_PREFIX.length(), columnName.length() - 1);
            return Map.of("approximation", Map.of("type", "confidence_interval", "column", sourceColumn));
        }
        if (columnName.startsWith(CERTIFIED_COLUMN_PREFIX) && columnName.endsWith(")")) {
            String sourceColumn = columnName.substring(CERTIFIED_COLUMN_PREFIX.length(), columnName.length() - 1);
            return Map.of("approximation", Map.of("type", "certified", "column", sourceColumn));
        }
        return null;
    }

    /**
     * The number of times (trials) the sampled rows are divided into buckets.
     */
    static final int TRIAL_COUNT = 2;

    /**
     * The number of buckets to use for computing confidence intervals.
     */
    public static final int BUCKET_COUNT = 16;

    /**
     * For grouped statistics (STATS ... BY), a grouping needs at least this
     * number of sampled rows to be included in the results. For a simple
     * aggregation as count, this still leads to acceptable confidence
     * intervals. When the number of rows is closed to the limit, a result
     * will never be marked as "certified reliable", because all buckets need
     * to data for that.
     */
    private static final int MIN_ROW_COUNT_FOR_RESULT_INCLUSION = 10;

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
     * Returns whether the logical plan is an approximation plan.
     */
    public static boolean is(LogicalPlan logicalPlan) {
        return logicalPlan.anyMatch(plan -> plan instanceof SampledAggregate);
    }

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
     *             | EVAL x = 2*x
     *             | EVAL bucketId = MV_APPEND(RANDOM(B), ... , RANDOM(B))  // T times
     *             | SAMPLED_STATS[SampleProbabilityPlaceHolder]
     *                     sampleSize = COUNT(*),
     *                     s = SUM(x),
     *                     `s$0` = SUM(x) WHERE MV_SLICE(bucketId, 0, 0) == 0
     *                     ...,
     *                     `s$T*B-1` = SUM(x) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *               BY group
     *             | WHERE sampleSize >= MIN_ROW_COUNT_FOR_RESULT_INCLUSION / prob
     *             | EVAL t = s*s, `t$0` = `s$0`*`s$0`, ..., `t$T*B-1` = `s$T*B-1`*`s$T*B-1`
     *             | EVAL `CONFIDENCE_INTERVAL(s)` = CONFIDENCE_INTERVAL(s, MV_APPEND(`s$0`, ... `s$T*B-1`), T, B, 0.90),
     *                    `CONFIDENCE_INTERVAL(t)` = CONFIDENCE_INTERVAL(t, MV_APPEND(`t$0`, ... `t$T*B-1`), T, B, 0.90)
     *             | KEEP s, t, `CONFIDENCE_INTERVAL(s)`, `CONFIDENCE_INTERVAL(t)`
     *     }
     * </pre>
     * During execution the {@code SAMPLED_STATS} is replaced on the data node by either
     * sampling the source rows and a normal {@code STATS} (with sample corrections applied
     * to intermediate state), or pushed down to Lucene without any sampling (if possible).
     */
    public static LogicalPlan get(LogicalPlan logicalPlan, ApproximationSettings settings) {
        logger.debug("generating approximation plan");

        Double confidenceLevel = settings.confidenceLevel();

        // Whether of not the first STATS command has been encountered yet.
        Holder<Boolean> encounteredStats = new Holder<>(false);

        // The keys are the IDs of the fields that have buckets. Confidence intervals are computed
        // for these fields at the end of the computation. They map to the list of buckets for
        // that field.
        // If confidenceLevel is null, no buckets are needed, and this map is not used.
        Map<NameId, List<Attribute>> fieldBuckets = confidenceLevel != null ? new HashMap<>() : null;

        // For each rounded expression, also keep track of the not rounded expression.
        // These are used when a division between two rounded expressions is encountered.
        // This results in more accurate values, because no round-off errors occur in
        // the numerator and denominator. The most common use case is AVG, which
        // is rewritten to AVG::double = SUM::double / COUNT::long.
        Map<NameId, Attribute> notRoundedExpressions = new HashMap<>();

        LogicalPlan approximationPlan = logicalPlan.transformUp(plan -> {
            if (encounteredStats.get() == false) {
                if (plan instanceof Aggregate == false) {
                    // Commands before the first STATS function should be left unchanged.
                    return plan;
                } else {
                    // The first STATS function should be replaced by a STATS with buckets
                    // (for computing confidence intervals).
                    encounteredStats.set(true);
                    return sampleCorrectedAggregateAndBuckets((Aggregate) plan, fieldBuckets, notRoundedExpressions);
                }
            } else {
                // After the STATS function, any processing of fields that have buckets, should
                // also process the buckets, so that confidence intervals for the dependent fields
                // can be computed.
                return planIncludingBuckets(plan, fieldBuckets, notRoundedExpressions);
            }
        });

        // Add the confidence intervals for all fields with buckets.
        if (confidenceLevel != null) {
            List<Alias> confidenceIntervals = getConfidenceIntervals(approximationPlan, fieldBuckets, confidenceLevel);
            approximationPlan = new Eval(Source.EMPTY, approximationPlan, confidenceIntervals);
        }

        // Drop all bucket fields and uncorrected fields from the output.
        Set<Attribute> dropAttributes = new HashSet<>(notRoundedExpressions.values());
        if (fieldBuckets != null) {
            dropAttributes.addAll(fieldBuckets.values().stream().flatMap(List::stream).toList());
        }

        List<Attribute> keepAttributes = new ArrayList<>(approximationPlan.output());
        keepAttributes.removeAll(dropAttributes);
        approximationPlan = new Project(Source.EMPTY, approximationPlan, keepAttributes);
        logger.debug("approximation plan:\n{}", approximationPlan);
        return approximationPlan;
    }

    /**
     * Replaces the aggregate by an aggregate with buckets (for confidence intervals),
     * and filters out groups with a too small sample size.
     * <p>
     * This means that:
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
     *                `s$T*B-1` = SUM(x) WHERE MV_SLICE(bucketId, T-1, T-1) == B-1
     *          BY group
     *          | WHERE sampleSize >= MIN_ROW_COUNT_FOR_RESULT_INCLUSION / prob
     *          | DROP sampleSize
     *      }
     * </pre>
     */
    private static LogicalPlan sampleCorrectedAggregateAndBuckets(
        Aggregate aggregate,
        Map<NameId, List<Attribute>> fieldBuckets,
        Map<NameId, Attribute> notRoundedExpressions
    ) {
        Expression sampleProbability = new SampleProbabilityPlaceHolder(Source.EMPTY);

        Expression randomBucketId = new Random(Source.EMPTY, Literal.integer(Source.EMPTY, BUCKET_COUNT));
        Expression bucketIds = randomBucketId;
        for (int trialId = 1; trialId < TRIAL_COUNT; trialId++) {
            bucketIds = new MvAppend(Source.EMPTY, bucketIds, randomBucketId);
        }
        Alias bucketIdField = new Alias(Source.EMPTY, BUCKET_ID_COLUMN_NAME, bucketIds);

        // The aggregate functions in the approximation plan.
        List<NamedExpression> bucketAggregates = new ArrayList<>();

        // List of expression that must be evaluated before the sampled aggregation.
        // For integer SUMs, the field must be cast to double before the aggregation.
        List<Alias> preEvals = new ArrayList<>();
        // List of expressions that must be evaluated after the sampled aggregation.
        // For COUNT, zeroes must be replaced by NULLs for the confidence interval computation.
        // For COUNT and integer SUMs, the sample-corrected double result must be
        // rounded and cast back to the original integer type.
        List<Alias> postEvals = new ArrayList<>();
        List<NamedExpression> originalAggregates = new ArrayList<>();
        List<Attribute> projections = new ArrayList<>();

        for (NamedExpression aggOrKey : aggregate.aggregates()) {
            if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                // This is a grouping key, not an aggregate function.
                originalAggregates.add(aggOrKey);
                projections.add(aggOrKey.toAttribute());
                continue;
            }

            Alias agg = (Alias) aggOrKey;
            AggregateFunction aggFn = (AggregateFunction) agg.child();

            // Double-precision version of the aggregate function if needed, so that
            // sample correction (dividing by the sample probability) on data nodes
            // stays in floating point and avoids round-off errors from integer
            // rounding.
            boolean needsRounding;
            if (aggFn instanceof Count count) {
                aggFn = new CountApproximate(count.source(), count.field(), count.filter(), count.window());
                needsRounding = true;
            } else if (aggFn instanceof Sum sum && sum.dataType().isWholeNumber()) {
                Alias doubleField = new Alias(
                    Source.EMPTY,
                    Attribute.rawTemporaryName(agg.name(), "todouble"),
                    new ToDouble(Source.EMPTY, sum.field())
                );
                preEvals.add(doubleField);
                aggFn = new Sum(sum.source(), doubleField.toAttribute(), sum.filter(), sum.window(), sum.summationMode());
                needsRounding = true;
            } else {
                needsRounding = false;
            }

            projections.add(agg.toAttribute());
            if (needsRounding) {
                Alias notRoundedAgg = new Alias(Source.EMPTY, Attribute.rawTemporaryName(agg.name() + "notrounded"), aggFn);
                notRoundedExpressions.put(agg.id(), notRoundedAgg.toAttribute());
                postEvals.add(agg.replaceChild(new ToLong(Source.EMPTY, notRoundedAgg.toAttribute())));
                agg = notRoundedAgg;
            }
            originalAggregates.add(agg);

            if (fieldBuckets != null && Approximation.SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass())) {
                // For the supported single-valued aggregations, add buckets with sampled
                // values, that will be used to compute a confidence interval.
                // For multivalued aggregations, confidence intervals do not make sense.
                List<Attribute> buckets = new ArrayList<>();
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
                        Alias bucket = new Alias(
                            Source.EMPTY,
                            Attribute.rawTemporaryName(aggOrKey.name(), "bucket", Integer.toString(trialId * BUCKET_COUNT + bucketId)),
                            aggFn.withFilter(
                                aggFn.hasFilter() == false ? bucketIdFilter : new And(Source.EMPTY, aggFn.filter(), bucketIdFilter)
                            )
                        );

                        if (needsRounding) {
                            Alias approxBucket = new Alias(
                                Source.EMPTY,
                                Attribute.rawTemporaryName(bucket.name(), "notrounded"),
                                bucket.child()
                            );
                            notRoundedExpressions.put(bucket.id(), approxBucket.toAttribute());
                            postEvals.add(bucket.replaceChild(new ToLong(Source.EMPTY, approxBucket.toAttribute())));
                            bucket = approxBucket;
                        }
                        bucketAggregates.add(bucket);

                        if (aggFn instanceof Count) {
                            // COUNT returns 0 for no data, but confidence computation needs NULL.
                            bucket = new Alias(
                                Source.EMPTY,
                                bucket.name(),
                                new Case(
                                    Source.EMPTY,
                                    new Equals(Source.EMPTY, bucket.toAttribute(), Literal.fromDouble(Source.EMPTY, 0.0)),
                                    List.of(Literal.NULL, bucket.toAttribute())
                                )
                            );
                            postEvals.add(bucket);
                        }
                        buckets.add(bucket.toAttribute());
                        projections.add(bucket.toAttribute());
                    }
                }
                fieldBuckets.put(aggOrKey.id(), buckets);
            }
        }

        List<NamedExpression> aggregates = new ArrayList<>(originalAggregates);
        aggregates.addAll(bucketAggregates);

        Alias sampleSize = null;
        if (aggregate.groupings().isEmpty() == false) {
            // Add the sample size per grouping to filter out groups with too few sampled rows.
            sampleSize = new Alias(
                Source.EMPTY,
                Attribute.rawTemporaryName("approximation", "sample_size"),
                new CountApproximate(Source.EMPTY, Literal.keyword(Source.EMPTY, StringUtils.WILDCARD))
            );
            aggregates.add(sampleSize);
            originalAggregates.add(sampleSize);
        }

        // Add the bucket ID, do the aggregations (including the buckets),
        // and filter out rows with too few sampled values.
        LogicalPlan plan = new Eval(Source.EMPTY, aggregate.child(), List.of(bucketIdField));
        if (preEvals.isEmpty() == false) {
            plan = new Eval(Source.EMPTY, plan, preEvals);
        }

        plan = new SampledAggregate(aggregate.source(), plan, aggregate.groupings(), aggregates, originalAggregates, sampleProbability);

        if (sampleSize != null) {
            // The sampleSize is sampled-corrected, so we have to multiply by the sample
            // probability to get the actual number of sampled rows.
            plan = new Filter(
                Source.EMPTY,
                plan,
                new Or(
                    Source.EMPTY,
                    new Equals(Source.EMPTY, sampleProbability, Literal.fromDouble(Source.EMPTY, 1.0)),
                    new GreaterThanOrEqual(
                        Source.EMPTY,
                        new Mul(Source.EMPTY, sampleSize.toAttribute(), sampleProbability),
                        Literal.fromDouble(Source.EMPTY, MIN_ROW_COUNT_FOR_RESULT_INCLUSION - 0.5)
                    )
                )
            );
        }

        plan = new Eval(Source.EMPTY, plan, postEvals);
        return new Project(Source.EMPTY, plan, projections);
    }

    /**
     * Returns a plan that also processes the buckets for fields that have them.
     * Luckily, there's only a limited set of commands that have to do something
     * with the buckets: EVAL, PROJECT and MV_EXPAND.
     */
    private static LogicalPlan planIncludingBuckets(
        LogicalPlan plan,
        Map<NameId, List<Attribute>> fieldBuckets,
        Map<NameId, Attribute> notRoundedExpressions
    ) {
        return switch (plan) {
            case Eval eval -> evalIncludingBuckets(eval, fieldBuckets, notRoundedExpressions);
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
        Map<NameId, List<Attribute>> fieldBuckets,
        Map<NameId, Attribute> notRoundedExpressions
    ) {
        List<Alias> fields = new ArrayList<>(eval.fields());
        if (fieldBuckets != null) {
            for (Alias field : eval.fields()) {
                // Don't create buckets for non-numeric or multivalued fields.
                if (field.dataType().isNumeric() == false
                    || field.child().anyMatch(expr -> MULTIVALUED_OUTPUT_FUNCTIONS.contains(expr.getClass()))) {
                    continue;
                }
                // If any of the field's dependencies has buckets, create buckets for this field as well.
                if (field.child().anyMatch(e -> e instanceof NamedExpression ne && fieldBuckets.containsKey(ne.id()))) {
                    List<Attribute> buckets = new ArrayList<>();
                    for (int bucketId = 0; bucketId < TRIAL_COUNT * BUCKET_COUNT; bucketId++) {
                        final int finalBucketId = bucketId;
                        Alias bucket = new Alias(
                            Source.EMPTY,
                            Attribute.rawTemporaryName(field.name(), "bucket", Integer.toString(bucketId)),
                            field.child()
                                .transformDown(
                                    e -> e instanceof NamedExpression ne && fieldBuckets.containsKey(ne.id())
                                        ? fieldBuckets.get(ne.id()).get(finalBucketId)
                                        : e
                                )
                        );
                        fields.add(bucket);
                        buckets.add(bucket.toAttribute());
                    }
                    fieldBuckets.put(field.id(), buckets);
                }
            }
        }

        // For each noninteger division of expressions, use not-rounded values.
        for (int i = 0; i < fields.size(); i++) {
            Alias field = fields.get(i);
            fields.set(i, field.replaceChild(field.child().transformUp(e -> {
                if (e instanceof Div div && div.dataType().isRationalNumber()) {
                    Attribute notRoundedLhs = div.left() instanceof NamedExpression left ? notRoundedExpressions.get(left.id()) : null;
                    Attribute notRoundedRhs = div.right() instanceof NamedExpression right ? notRoundedExpressions.get(right.id()) : null;
                    if (notRoundedLhs == null && notRoundedRhs == null) {
                        return div;
                    } else {
                        return new Div(
                            div.source(),
                            notRoundedLhs != null ? notRoundedLhs : div.left(),
                            notRoundedRhs != null ? notRoundedRhs : div.right(),
                            div.dataType()
                        );
                    }
                } else {
                    return e;
                }
            })));
        }

        return new Eval(Source.EMPTY, eval.child(), fields);
    }

    /**
     * For PROJECT, if it renames a field with buckets, add the renamed field
     * to the map of fields with buckets.
     */
    private static LogicalPlan projectIncludingBuckets(Project project, Map<NameId, List<Attribute>> fieldBuckets) {
        if (fieldBuckets == null) {
            return project;
        }

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
                projections.addAll(fieldBuckets.get(projection.id()));
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
    private static LogicalPlan mvExpandIncludingBuckets(MvExpand mvExpand, Map<NameId, List<Attribute>> fieldBuckets) {
        if (fieldBuckets != null && fieldBuckets.containsKey(mvExpand.target().id())) {
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
        Map<NameId, List<Attribute>> fieldBuckets,
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
                List<Attribute> buckets = fieldBuckets.get(output.id());
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
                    Expression bucket = buckets.get(i);
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
                        CONFIDENCE_INTERVAL_COLUMN_PREFIX + output.name() + ")",
                        new MvSlice(Source.EMPTY, confidenceInterval, Literal.integer(Source.EMPTY, 0), Literal.integer(Source.EMPTY, 1)),
                        null,
                        true
                    )
                );
                confidenceIntervalsAndCertified.add(
                    new Alias(
                        Source.EMPTY,
                        CERTIFIED_COLUMN_PREFIX + output.name() + ")",
                        new GreaterThanOrEqual(
                            Source.EMPTY,
                            new MvSlice(
                                Source.EMPTY,
                                confidenceInterval,
                                Literal.integer(Source.EMPTY, 2),
                                Literal.integer(Source.EMPTY, 2)
                            ),
                            Literal.fromDouble(Source.EMPTY, 0.5)
                        ),
                        null,
                        true
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
