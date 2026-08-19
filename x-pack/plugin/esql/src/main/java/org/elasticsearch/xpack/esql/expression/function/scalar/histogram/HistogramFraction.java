/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.TDigestStates;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.TDigest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.ToRange;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Estimates the number of values of a histogram that fall into a given range (bucket).
 * Because the count is estimated and might involve interpolation, the result can be fractional.
 * The optional {@code decimals} argument defines the number of decimal places to round the result to
 * (with the same semantics as {@code ROUND}); if it is absent, no rounding is performed.
 * The rounding is performed in a way so that rounding errors amortize across adjacent buckets instead of accumulating.
 * Note that this function is currently only intended for internal usage and not available as a user-facing ES|QL function.
 * Therefore, it is intentionally not registered in {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 * It does however back the PromQL {@code histogram_fraction()} function via {@link #PROMQL_DEFINITION}, which normalizes
 * the absolute count returned by this function into the fraction of the total observation count that PromQL expects.
 */
public class HistogramFraction extends EsqlScalarFunction implements OptionalArgument, AnyNullIsNull {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramFraction",
        HistogramFraction::new
    );

    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .histogramTernary(
            PromqlFunctionDefinition.LOWER_SCALAR,
            PromqlFunctionDefinition.UPPER_SCALAR,
            (source, target, ctx, extraParams) -> {
                Expression lower = new ToDouble(source, extraParams.get(0));
                Expression upper = new ToDouble(source, extraParams.get(1));
                Expression fraction = new HistogramFraction(source, target, new ToRange(source, lower, upper), null);
                Expression count = ExtractHistogramComponent.create(source, target, ExponentialHistogramBlock.Component.COUNT);
                // The ES|QL function returns the absolute number of observations in the bucket, while PromQL returns
                // the fraction of the total observation count, so normalize by dividing through the total count.
                // Guard against empty histograms to return null instead of dividing by zero.
                return new Case(
                    source,
                    new GreaterThan(source, count, Literal.fromDouble(source, 0.0), null),
                    List.of(new Div(source, fraction, count))
                );
            }
        )
        .classicHistogramHandler(org.elasticsearch.xpack.esql.plan.logical.promql.HistogramFraction::new)
        .description(
            "Returns the estimated fraction of observations of a classic or native histogram that fall between the provided "
                + "lower and upper values."
        )
        .example("histogram_fraction(0, 0.2, increase(http_request_duration_seconds[1h]))")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .name("histogram_fraction");

    private final Expression histogram;
    private final Expression bucket;
    private final Expression decimals;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = { "double" },
        briefSummary = "Estimates the number of values of a histogram that fall into a given range."
    )
    public HistogramFraction(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram", "tdigest" }) Expression histogram,
        @Param(name = "bucket", type = { "double_range" }) Expression bucket,
        @Param(
            optional = true,
            name = "decimals",
            type = { "integer" },
            description = "The number of decimal places to round to, must be a non-null literal. If absent, no rounding is performed."
        ) Expression decimals
    ) {
        super(source, decimals != null ? List.of(histogram, bucket, decimals) : List.of(histogram, bucket));
        this.histogram = histogram;
        this.bucket = bucket;
        this.decimals = decimals;
    }

    private HistogramFraction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    Expression histogram() {
        return histogram;
    }

    Expression bucket() {
        return bucket;
    }

    Expression decimals() {
        return decimals;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isType(
            histogram,
            dt -> dt == DataType.EXPONENTIAL_HISTOGRAM || dt == DataType.TDIGEST,
            sourceText(),
            FIRST,
            "exponential_histogram",
            "tdigest"
        ).and(isType(bucket, dt -> dt == DataType.DOUBLE_RANGE, sourceText(), SECOND, "double_range"));
        if (decimals != null) {
            resolution = resolution.and(isType(decimals, dt -> dt == DataType.INTEGER, sourceText(), THIRD, "integer"))
                .and(isNotNull(decimals, sourceText(), THIRD))
                .and(isFoldable(decimals, sourceText(), THIRD));
        }
        return resolution;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new HistogramFraction(source(), newChildren.get(0), newChildren.get(1), decimals == null ? null : newChildren.get(2));
    }

    @Override
    public boolean foldable() {
        return histogram.foldable() && bucket.foldable() && (decimals == null || decimals.foldable());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, HistogramFraction::new, histogram, bucket, decimals);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(histogram);
        out.writeNamedWriteable(bucket);
        out.writeOptionalNamedWriteable(decimals);
    }

    @Evaluator(extraName = "ExponentialHistogram")
    static double process(ExponentialHistogram histogram, DoubleRangeBlockBuilder.DoubleRange bucket, @Fixed Integer decimals) {
        if (histogram.valueCount() == 0) {
            return 0.0;
        }
        double lowerCount = ExponentialHistogramQuantile.estimateRank(histogram, bucket.from(), false);
        double upperCount = ExponentialHistogramQuantile.estimateRank(histogram, bucket.to(), false);
        if (decimals == null) {
            return upperCount - lowerCount;
        } else {
            return Maths.round(upperCount, decimals).doubleValue() - Maths.round(lowerCount, decimals).doubleValue();
        }
    }

    @Evaluator(extraName = "TDigest")
    static double process(
        TDigestHolder histogram,
        DoubleRangeBlockBuilder.DoubleRange bucket,
        @Fixed Integer decimals,
        @Fixed(scope = Fixed.Scope.THREAD_LOCAL) MemoryTrackingTDigestArrays tdigestArrays
    ) {
        try (TDigest scratch = TDigest.createMergingDigest(tdigestArrays, TDigestStates.COMPRESSION)) {
            scratch.add(histogram);
            long totalNumValues = scratch.size();
            if (totalNumValues == 0) {
                return 0.0;
            }
            double lowerCount = scratch.cdf(bucket.from()) * totalNumValues;
            double upperCount = scratch.cdf(bucket.to()) * totalNumValues;
            if (decimals == null) {
                return upperCount - lowerCount;
            } else {
                return Maths.round(upperCount, decimals).doubleValue() - Maths.round(lowerCount, decimals).doubleValue();
            }
        }
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var histogramEvaluator = toEvaluator.apply(histogram);
        var bucketEvaluator = toEvaluator.apply(bucket);
        // decimals is enforced to be a literal in resolveType(), so it can be folded here and baked into the evaluator.
        // A null value here means the optional argument was omitted.
        Integer decimalsValue = decimals == null ? null : (Integer) decimals.fold(toEvaluator.foldCtx());
        DataType histogramType = histogram.dataType();
        return switch (histogramType) {
            case EXPONENTIAL_HISTOGRAM -> new HistogramFractionExponentialHistogramEvaluator.Factory(
                source(),
                histogramEvaluator,
                bucketEvaluator,
                decimalsValue
            );
            case TDIGEST -> new HistogramFractionTDigestEvaluator.Factory(
                source(),
                histogramEvaluator,
                bucketEvaluator,
                decimalsValue,
                driverContext -> new MemoryTrackingTDigestArrays(driverContext.breaker())
            );
            default -> throw EsqlIllegalArgumentException.illegalDataType(histogramType);
        };
    }
}
