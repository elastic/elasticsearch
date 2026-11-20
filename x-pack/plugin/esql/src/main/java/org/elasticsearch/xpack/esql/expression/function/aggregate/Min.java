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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinNanPropagatingDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinNonFiniteDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.unit.UnitAware;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Min extends UnaryAggregateFunction
    implements
        ToAggregator,
        SurrogateExpression,
        AggregateMetricDoubleNativeSupport,
        AnyNullIsNull,
        NonFiniteSupport,
        UnitAware.PreservesUnit {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Min", Min::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Min.class).unary(Min::new).name("min");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // PromQL requires IEEE-754 semantics: NaN is skipped unless all inputs are NaN, and ±Inf are ordinary values.
        .acrossSeries((source, field) -> new Min(source, field, true))
        .description("Returns the minimum value across the input vector.")
        .example("min(http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("min");

    /**
     * First transport version whose {@code MIN} and {@code MAX} over {@code double} skip {@code NaN}. A node that predates it
     * makes the result {@code NaN} when any input is {@code NaN}, and combines partial results the same way, so a plan that
     * reaches such a node must keep the old rule everywhere. See {@link #forTransportVersion}.
     */
    public static final TransportVersion ESQL_MINMAX_SKIP_NAN = TransportVersion.fromName("esql_minmax_skip_nan");

    /**
     * When {@code true}, the {@code double} minimum uses Prometheus non-finite semantics (NaN skipped unless all inputs
     * are NaN). Set only by the PromQL translation; native ES|QL {@code MIN} uses the default aggregator, which also skips
     * NaN but orders -0.0 below 0.0.
     */
    private final boolean allowNonFinite;

    /**
     * Whether the {@code double} minimum skips {@code NaN}. {@code false} by default, the behaviour of nodes that
     * predate {@link ESQL_MINMAX_SKIP_NAN}; {@link #forTransportVersion} switches it on once every node supports it. Not used by the
     * PromQL variant, whose aggregator always skips NaN.
     */
    private final boolean skipNan;

    private static final Map<DataType, Supplier<AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, MinBooleanAggregatorFunctionSupplier::new),
        Map.entry(DataType.LONG, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.UNSIGNED_LONG, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATE_NANOS, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.INTEGER, MinIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, MinDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, MinIpAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, MinBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, MinBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, MinBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = { "boolean", "double", "integer", "long", "date", "date_nanos", "ip", "keyword", "unsigned_long", "version" },
        briefSummary = "Returns the minimum value of a field.",
        description = "The minimum value of a field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "min"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the minimum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MIN` function",
                file = "stats",
                tag = "docsStatsMinNestedExpression"
            ),
            @Example(
                description = "`MIN` can also operate on `exponential_histogram` fields, "
                    + "returning the minimum of the values which were used to construct the histograms.",
                file = "exponential_histogram",
                tag = "minExpHistoForDocs"
            ),
            @Example(
                description = "`MIN` can also operate on `tdigest` and casted `histogram` fields, "
                    + "returning the minimum of the values which were used to construct the digests.",
                file = "tdigest",
                tag = "minTDigestForDocs"
            ) }
    )
    public Min(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "double",
                "integer",
                "long",
                "date",
                "date_nanos",
                "ip",
                "keyword",
                "text",
                "unsigned_long",
                "version",
                "exponential_histogram",
                "tdigest" }
        ) Expression field
    ) {
        this(source, field, false);
    }

    /**
     * Builds a {@code Min} that uses Prometheus non-finite semantics when {@code allowNonFinite} is true. Used by the
     * PromQL translation; native ES|QL {@code MIN} uses the default aggregator.
     */
    public Min(Source source, Expression field, boolean allowNonFinite) {
        this(source, field, Literal.TRUE, NO_WINDOW, allowNonFinite);
    }

    public Min(Source source, Expression field, Expression filter, Expression window) {
        this(source, field, filter, window, false);
    }

    public Min(Source source, Expression field, Expression filter, Expression window, boolean allowNonFinite) {
        this(source, field, filter, window, allowNonFinite, false);
    }

    public Min(Source source, Expression field, Expression filter, Expression window, boolean allowNonFinite, boolean skipNan) {
        super(source, field, filter, window, emptyList());
        this.allowNonFinite = allowNonFinite;
        this.skipNan = skipNan;
    }

    private Min(StreamInput in) throws IOException {
        super(in);
        this.allowNonFinite = NonFiniteSupport.readNonFinite(in, NonFiniteSupport.ESQL_PROMQL_NON_FINITE_MAX_MIN);
        this.skipNan = in.getTransportVersion().supports(ESQL_MINMAX_SKIP_NAN) && in.readBoolean();
    }

    @Override
    public TransportVersion nonFiniteTransportVersion() {
        return NonFiniteSupport.ESQL_PROMQL_NON_FINITE_MAX_MIN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The non-finite flag, when present, follows the base fields; version-gated so older nodes never see the byte.
        super.writeTo(out);
        writeNonFinite(out);
        if (out.getTransportVersion().supports(ESQL_MINMAX_SKIP_NAN)) {
            out.writeBoolean(skipNan);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    public boolean skipNan() {
        return skipNan;
    }

    @Override
    public Expression toStrictVariant() {
        return new Min(source(), field(), filter(), window(), false, skipNan);
    }

    /**
     * Downgrades the PromQL variant when a node predates it, then switches on {@code NaN} skipping when every node supports
     * it. Both steps are returned together, because the substitution does not visit its own result again.
     */
    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        Min result = this;
        if (allowNonFinite && minTransportVersion.supports(nonFiniteTransportVersion()) == false) {
            result = (Min) toStrictVariant();
        }
        if (result.allowNonFinite == false && result.skipNan == false && minTransportVersion.supports(ESQL_MINMAX_SKIP_NAN)) {
            result = new Min(source(), field(), filter(), window(), false, true);
        }
        return result == this ? null : result;
    }

    /**
     * The flags take part in equality because a transformation only keeps a rebuilt child when it is not equal to the
     * original, so a change that only flips a flag would otherwise never reach the plan.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && allowNonFinite == ((Min) obj).allowNonFinite && skipNan == ((Min) obj).skipNan;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), allowNonFinite, skipNan);
    }

    @Override
    protected NodeInfo<Min> info() {
        return NodeInfo.create(this, Min::new, field(), filter(), window(), allowNonFinite, skipNan);
    }

    @Override
    public Min replaceChildren(List<Expression> newChildren) {
        return new Min(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), allowNonFinite, skipNan);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            dt -> SUPPLIERS.containsKey(dt)
                || dt == DataType.AGGREGATE_METRIC_DOUBLE
                || dt == DataType.EXPONENTIAL_HISTOGRAM
                || dt == DataType.TDIGEST,
            sourceText(),
            DEFAULT,
            "boolean",
            "date",
            "ip",
            "string",
            "version",
            "aggregate_metric_double",
            "exponential_histogram",
            "tdigest",
            "numeric except counter types"
        );
    }

    @Override
    public DataType dataType() {
        if (field().dataType() == DataType.AGGREGATE_METRIC_DOUBLE
            || field().dataType() == DataType.EXPONENTIAL_HISTOGRAM
            || field().dataType() == DataType.TDIGEST) {
            return DataType.DOUBLE;
        }
        return field().dataType().noText();
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        // The PromQL value column is always a double, so only the double path has a non-finite variant.
        if (allowNonFinite && type == DataType.DOUBLE) {
            return new MinNonFiniteDoubleAggregatorFunctionSupplier();
        }
        if (skipNan == false && type == DataType.DOUBLE) {
            return new MinNanPropagatingDoubleAggregatorFunctionSupplier();
        }
        if (SUPPLIERS.containsKey(type) == false) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return SUPPLIERS.get(type).get();
    }

    @Override
    public Expression surrogate() {
        if (field().dataType() == DataType.AGGREGATE_METRIC_DOUBLE) {
            return new Min(
                source(),
                FromAggregateMetricDouble.withMetric(source(), field(), AggregateMetricDoubleBlockBuilder.Metric.MIN),
                filter(),
                window(),
                allowNonFinite,
                skipNan
            );
        }
        if (field().dataType() == DataType.EXPONENTIAL_HISTOGRAM || field().dataType() == DataType.TDIGEST) {
            return new Min(
                source(),
                ExtractHistogramComponent.create(source(), field(), HistogramBlock.Component.MIN),
                filter(),
                window(),
                allowNonFinite,
                skipNan
            );
        }
        return field().foldable() ? new MvMin(source(), field()) : null;
    }
}
