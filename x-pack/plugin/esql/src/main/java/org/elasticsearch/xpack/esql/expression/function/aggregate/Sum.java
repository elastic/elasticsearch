/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LossySumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDenseVectorAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumOverflowingLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate implements SurrogateExpression, TransportVersionAware, AggregateMetricDoubleNativeSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sum", Sum::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Sum.class).unary(Sum::new).name("sum");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .acrossSeries(Sum::new)
        .description("Calculates the sum of the values across the input vector.")
        .example("sum(http_requests_total)")
        .name("sum");

    public static final TransportVersion ESQL_SUM_LONG_OVERFLOW_FIX = TransportVersion.fromName("esql_sum_long_overflow_fix");

    /**
     * Mode for {@link #longOverflowMode}, throwing a 500 error on long overflows.
     * <p>
     *     Old pre-{@link #ESQL_SUM_LONG_OVERFLOW_FIX} behavior.
     * </p>
     * <p>
     *     Used by default, and replaced with {@link #LONG_OVERFLOW_WARN} in {@link #forTransportVersion}.
     * </p>
     */
    public static final Literal LONG_OVERFLOW_THROW = Literal.keyword(Source.EMPTY, "long_overflow_throw");
    /**
     * Mode for {@link #longOverflowMode}, returning a null and a warning on long overflows.
     * <p>
     *     New behavior added in {@link #ESQL_SUM_LONG_OVERFLOW_FIX}.
     * </p>
     */
    public static final Literal LONG_OVERFLOW_WARN = Literal.keyword(Source.EMPTY, "long_overflow_warn");

    private final Expression summationMode;
    /**
     * Either {@link #LONG_OVERFLOW_THROW} or {@link #LONG_OVERFLOW_WARN}.
     * Set internally only, used for BWC.
     */
    private final Expression longOverflowMode;

    @FunctionInfo(
        returnType = { "long", "double", "dense_vector" },
        description = "The sum of a numeric expression.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "sum"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate "
                    + "the sum of each employee’s maximum salary changes, apply the "
                    + "`MV_MAX` function to each row and then sum the results",
                file = "stats",
                tag = "docsStatsSumNestedExpression"
            ),
            @Example(
                description = "`SUM` can also operate on `exponential_histogram` fields, "
                    + "computing the sum of the values which were used to construct the histograms.",
                file = "exponential_histogram",
                tag = "sumExpHistoForDocs"
            ),
            @Example(
                description = "`SUM` can also operate on `tdigest` and casted `histogram` fields, "
                    + "computing the sum of the values which were used to construct the digests.",
                file = "tdigest",
                tag = "sumTDigestForDocs"
            ) }
    )
    public Sum(
        Source source,
        @Param(
            name = "number",
            type = { "aggregate_metric_double", "exponential_histogram", "tdigest", "double", "integer", "long", "dense_vector" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, SummationMode.COMPENSATED_LITERAL);
    }

    public Sum(Source source, Expression field, Expression filter, Expression window, Expression summationMode) {
        this(source, field, filter, window, summationMode, LONG_OVERFLOW_THROW);
    }

    public Sum(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression summationMode,
        Expression longOverflowMode
    ) {
        super(source, field, filter, window, List.of(summationMode, longOverflowMode));
        this.summationMode = summationMode;
        this.longOverflowMode = longOverflowMode;
    }

    private Sum(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            readParameters(in)
        );
    }

    private record SumParameters(Expression summationMode, Expression longOverflowMode) {}

    private Sum(Source source, Expression field, Expression filter, Expression window, SumParameters params) {
        this(source, field, filter, window, params.summationMode(), params.longOverflowMode());
    }

    private static SumParameters readParameters(StreamInput in) throws IOException {
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression summationMode = parameters.isEmpty() ? SummationMode.COMPENSATED_LITERAL : parameters.getFirst();
        Expression overflowing = parameters.size() >= 2 ? parameters.get(1) : LONG_OVERFLOW_THROW;
        return new SumParameters(summationMode, overflowing);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Sum> info() {
        return NodeInfo.create(this, Sum::new, field(), filter(), window(), summationMode, longOverflowMode);
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }

    @Override
    public Sum withFilter(Expression filter) {
        return new Sum(source(), field(), filter, window(), summationMode, longOverflowMode);
    }

    @Override
    public DataType dataType() {
        DataType dt = field().dataType();
        if (dt == DataType.NULL) {
            return DataType.NULL;
        }
        if (dt == DataType.DENSE_VECTOR) {
            return DataType.DENSE_VECTOR;
        }
        return dt.isWholeNumber() == false || dt == UNSIGNED_LONG ? DOUBLE : LONG;
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier() {
        if (longOverflowMode().equals(LONG_OVERFLOW_THROW)) {
            return new SumOverflowingLongAggregatorFunctionSupplier();
        }
        return new SumLongAggregatorFunctionSupplier(source());
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier() {
        return new SumIntAggregatorFunctionSupplier();
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier() {
        final SummationMode mode = SummationMode.fromLiteral(summationMode);
        return switch (mode) {
            case COMPENSATED -> new SumDoubleAggregatorFunctionSupplier();
            case LOSSY -> new LossySumDoubleAggregatorFunctionSupplier();
        };
    }

    @Override
    protected AggregatorFunctionSupplier denseVectorSupplier() {
        return new SumDenseVectorAggregatorFunctionSupplier(source());
    }

    public Expression summationMode() {
        return summationMode;
    }

    public Expression longOverflowMode() {
        return longOverflowMode;
    }

    @Override
    protected TypeResolution resolveType() {
        if (supportsDates()) {
            return TypeResolutions.isType(
                this,
                e -> e == DataType.DATETIME
                    || e == DataType.AGGREGATE_METRIC_DOUBLE
                    || e == DataType.EXPONENTIAL_HISTOGRAM
                    || e == DataType.TDIGEST
                    || e.isNumeric() && e != DataType.UNSIGNED_LONG,
                sourceText(),
                DEFAULT,
                "datetime",
                "aggregate_metric_double, exponential_histogram, tdigest or numeric except unsigned_long or counter types"
            );
        }
        return isType(
            field(),
            dt -> dt == DataType.AGGREGATE_METRIC_DOUBLE
                || dt == DataType.EXPONENTIAL_HISTOGRAM
                || dt == DataType.TDIGEST
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG)
                || dt == DataType.DENSE_VECTOR,
            sourceText(),
            DEFAULT,
            "aggregate_metric_double, exponential_histogram, tdigest or numeric except unsigned_long or counter types"
        );
    }

    @Override
    public Expression surrogate() {
        var field = field();
        if (field.dataType() == DataType.DENSE_VECTOR) {
            return null;
        }

        var s = source();
        if (field.dataType() == AGGREGATE_METRIC_DOUBLE) {
            return new Sum(
                s,
                FromAggregateMetricDouble.withMetric(source(), field, AggregateMetricDoubleBlockBuilder.Metric.SUM),
                filter(),
                window(),
                summationMode,
                longOverflowMode
            );
        }
        if (field.dataType() == EXPONENTIAL_HISTOGRAM || field.dataType() == DataType.TDIGEST) {
            return new Sum(
                s,
                ExtractHistogramComponent.create(source(), field, HistogramBlock.Component.SUM),
                filter(),
                window(),
                summationMode,
                longOverflowMode
            );
        }

        if (field.foldable()) {
            if (field().dataType() == NULL) {
                return new Literal(s, null, NULL);
            }
            // SUM(const) is equivalent to MV_SUM(const)*COUNT(*).
            return new Mul(s, new MvSum(s, field), new Count(s, Literal.keyword(s, StringUtils.WILDCARD), filter(), window()));
        } else {
            return null;
        }
    }

    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        if (minTransportVersion.supports(ESQL_SUM_LONG_OVERFLOW_FIX) && longOverflowMode().equals(LONG_OVERFLOW_THROW)) {
            return new Sum(source(), field(), filter(), window(), summationMode, LONG_OVERFLOW_WARN);
        }
        return null;
    }
}
