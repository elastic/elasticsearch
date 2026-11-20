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
import org.elasticsearch.compute.aggregation.MaxBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxNanPropagatingDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxNonFiniteDoubleAggregatorFunctionSupplier;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
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

public class Max extends UnaryAggregateFunction
    implements
        ToAggregator,
        SurrogateExpression,
        AggregateMetricDoubleNativeSupport,
        AnyNullIsNull,
        NonFiniteSupport,
        UnitAware.PreservesUnit {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Max", Max::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Max.class).unary(Max::new).name("max");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // PromQL requires IEEE-754 semantics: NaN is skipped unless all inputs are NaN, and ±Inf are ordinary values.
        .acrossSeries((source, field) -> new Max(source, field, true))
        .description("Returns the maximum value across the input vector.")
        .example("max(http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("max");

    /**
     * When {@code true}, the {@code double} maximum uses Prometheus non-finite semantics (NaN skipped unless all inputs
     * are NaN). Set only by the PromQL translation; native ES|QL {@code MAX} uses the default aggregator, which also skips
     * NaN but orders -0.0 below 0.0.
     */
    private final boolean allowNonFinite;

    /**
     * Whether the {@code double} maximum skips {@code NaN}. {@code false} by default, the behaviour of nodes that
     * predate {@link Min#ESQL_MINMAX_SKIP_NAN}; {@link #forTransportVersion} switches it on once every node supports it. Not used by the
     * PromQL variant, whose aggregator always skips NaN.
     */
    private final boolean skipNan;

    private static final Map<DataType, Supplier<AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, MaxBooleanAggregatorFunctionSupplier::new),
        Map.entry(DataType.LONG, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.UNSIGNED_LONG, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATE_NANOS, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.INTEGER, MaxIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, MaxDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, MaxIpAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, MaxBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = { "boolean", "double", "integer", "long", "date", "date_nanos", "ip", "keyword", "unsigned_long", "version" },
        briefSummary = "Returns the maximum value of a field.",
        description = "The maximum value of a field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "max"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the maximum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MAX` function",
                file = "stats",
                tag = "docsStatsMaxNestedExpression"
            ),
            @Example(
                description = "`MAX` can also operate on `exponential_histogram` fields, "
                    + "returning the maximum of the values which were used to construct the histograms.",
                file = "exponential_histogram",
                tag = "maxExpHistoForDocs"
            ),
            @Example(
                description = "`MAX` can also operate on `tdigest` and casted `histogram` fields, "
                    + "returning the maximum of the values which were used to construct the digests.",
                file = "tdigest",
                tag = "maxTDigestForDocs"
            ) }
    )
    public Max(
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
     * Builds a {@code Max} that uses Prometheus non-finite semantics when {@code allowNonFinite} is true. Used by the
     * PromQL translation; native ES|QL {@code MAX} uses the default aggregator.
     */
    public Max(Source source, Expression field, boolean allowNonFinite) {
        this(source, field, Literal.TRUE, NO_WINDOW, allowNonFinite);
    }

    public Max(Source source, Expression field, Expression filter, Expression window) {
        this(source, field, filter, window, false);
    }

    public Max(Source source, Expression field, Expression filter, Expression window, boolean allowNonFinite) {
        this(source, field, filter, window, allowNonFinite, false);
    }

    public Max(Source source, Expression field, Expression filter, Expression window, boolean allowNonFinite, boolean skipNan) {
        super(source, field, filter, window, emptyList());
        this.allowNonFinite = allowNonFinite;
        this.skipNan = skipNan;
    }

    private Max(StreamInput in) throws IOException {
        super(in);
        this.allowNonFinite = NonFiniteSupport.readNonFinite(in, NonFiniteSupport.ESQL_PROMQL_NON_FINITE_MAX_MIN);
        this.skipNan = in.getTransportVersion().supports(Min.ESQL_MINMAX_SKIP_NAN) && in.readBoolean();
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
        if (out.getTransportVersion().supports(Min.ESQL_MINMAX_SKIP_NAN)) {
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
        return new Max(source(), field(), filter(), window(), false, skipNan);
    }

    /**
     * Downgrades the PromQL variant when a node predates it, then switches on {@code NaN} skipping when every node supports
     * it. Both steps are returned together, because the substitution does not visit its own result again.
     */
    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        Max result = this;
        if (allowNonFinite && minTransportVersion.supports(nonFiniteTransportVersion()) == false) {
            result = (Max) toStrictVariant();
        }
        if (result.allowNonFinite == false && result.skipNan == false && minTransportVersion.supports(Min.ESQL_MINMAX_SKIP_NAN)) {
            result = new Max(source(), field(), filter(), window(), false, true);
        }
        return result == this ? null : result;
    }

    /**
     * The flags take part in equality because a transformation only keeps a rebuilt child when it is not equal to the
     * original, so a change that only flips a flag would otherwise never reach the plan.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && allowNonFinite == ((Max) obj).allowNonFinite && skipNan == ((Max) obj).skipNan;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), allowNonFinite, skipNan);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field(), filter(), window(), allowNonFinite, skipNan);
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), allowNonFinite, skipNan);
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
            return new MaxNonFiniteDoubleAggregatorFunctionSupplier();
        }
        if (skipNan == false && type == DataType.DOUBLE) {
            return new MaxNanPropagatingDoubleAggregatorFunctionSupplier();
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
            return new Max(
                source(),
                FromAggregateMetricDouble.withMetric(source(), field(), AggregateMetricDoubleBlockBuilder.Metric.MAX),
                filter(),
                window(),
                allowNonFinite,
                skipNan
            );
        }
        if (field().dataType() == DataType.EXPONENTIAL_HISTOGRAM || field().dataType() == DataType.TDIGEST) {
            return new Max(
                source(),
                ExtractHistogramComponent.create(source(), field(), HistogramBlock.Component.MAX),
                filter(),
                window(),
                allowNonFinite,
                skipNan
            );
        }
        return field().foldable() ? new MvMax(source(), field()) : null;
    }
}
