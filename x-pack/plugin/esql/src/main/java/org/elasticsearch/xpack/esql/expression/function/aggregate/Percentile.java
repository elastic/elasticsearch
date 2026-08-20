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
import org.elasticsearch.compute.aggregation.PercentileDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramPercentile;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentile;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.doubleValueOf;

public class Percentile extends NumericAggregate implements SurrogateExpression {
    private static final TransportVersion PERCENTILE_COMPRESSION = TransportVersion.fromName("esql_percentile_compression");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Percentile",
        Percentile::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Percentile.class).binary(Percentile::new).name("percentile");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .acrossSeriesBinary(
            PromqlFunctionDefinition.QUANTILE,
            (source, field, filter, window, phi) -> new Percentile(
                source,
                field,
                filter,
                window,
                PromqlFunctionDefinition.quantileToPercentile(source, phi)
            )
        )
        .description("Returns the φ-quantile (0 ≤ φ ≤ 1) of the values across the input vector.")
        .example("quantile(0.9, http_request_duration_seconds)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.QUANTILE_NOTE)
        .name("quantile");

    private final Expression percentile;
    private final double tDigestStateCompression;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "double",
        briefSummary = "Returns the value at which a certain percentage of observed values occur.",
        description = "Returns the value at which a certain percentage of observed values occur. "
            + "For example, the 95th percentile is the value which is greater than 95% of the "
            + "observed values and the 50th percentile is the `MEDIAN`.",
        appendix = """
            ### `PERCENTILE` is (usually) approximate [esql-percentile-approximate]

            :::{include} /reference/aggregations/_snippets/search-aggregations-metrics-percentile-aggregation-approximate.md
            :::

            ::::{warning}
            `PERCENTILE` is also {wikipedia}/Nondeterministic_algorithm[non-deterministic].
            This means you can get slightly different results using the same data.
            ::::""",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats_percentile", tag = "percentile"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate a percentile "
                    + "of the maximum values of a multivalued column, first use `MV_MAX` to get the "
                    + "maximum value per row, and use the result with the `PERCENTILE` function",
                file = "stats_percentile",
                tag = "docsStatsPercentileNestedExpression"
            ),
            @Example(
                description = "`PERCENTILE` can also operate on `exponential_histogram` fields, "
                    + "approximating the percentile of the values which were used to construct the histograms.",
                file = "exponential_histogram",
                tag = "percentileExpHistoForDocs"
            ),
            @Example(
                description = "`PERCENTILE` can also operate on `tdigest` and casted `histogram` fields, "
                    + "approximating the percentile of the values which were used to construct the digests.",
                file = "tdigest",
                tag = "percentileTDigestForDocs"
            ) }
    )
    public Percentile(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long", "exponential_histogram", "tdigest" }) Expression field,
        @Param(
            name = "percentile",
            type = { "double", "integer", "long" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT)
        ) Expression percentile
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, percentile);
    }

    public Percentile(Source source, Expression field, Expression filter, Expression window, Expression percentile) {
        this(source, field, filter, window, percentile, QuantileStates.DEFAULT_COMPRESSION);
    }

    public Percentile(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression percentile,
        double tDigestStateCompression
    ) {
        super(source, field, filter, window, singletonList(percentile));
        this.percentile = percentile;
        this.tDigestStateCompression = tDigestStateCompression;
    }

    private Percentile(StreamInput in) throws IOException {
        super(in);
        this.percentile = parameters().getFirst();
        this.tDigestStateCompression = in.getTransportVersion().supports(PERCENTILE_COMPRESSION)
            ? in.readDouble()
            : QuantileStates.DEFAULT_COMPRESSION;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(PERCENTILE_COMPRESSION)) {
            out.writeDouble(tDigestStateCompression);
        }
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), filter(), window(), percentile, tDigestStateCompression);
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        return new Percentile(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            tDigestStateCompression
        );
    }

    @Override
    public Percentile withFilter(Expression filter) {
        return new Percentile(source(), field(), filter, window(), percentile, tDigestStateCompression);
    }

    public Expression percentile() {
        return percentile;
    }

    public double tDigestStateCompression() {
        return tDigestStateCompression;
    }

    public Percentile withTDigestStateCompression(double newCompression) {
        return new Percentile(source(), field(), filter(), window(), percentile, newCompression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tDigestStateCompression);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        return Double.compare(((Percentile) obj).tDigestStateCompression, tDigestStateCompression) == 0;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            field(),
            dt -> (dt.isNumeric() && dt != DataType.UNSIGNED_LONG) || dt == DataType.EXPONENTIAL_HISTOGRAM || dt == DataType.TDIGEST,
            sourceText(),
            FIRST,
            "exponential_histogram, tdigest or numeric except unsigned_long"
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        return isType(
            percentile,
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            SECOND,
            "numeric except unsigned_long"
        ).and(isFoldable(percentile, sourceText(), SECOND)).and(isNotNull(percentile, sourceText(), SECOND));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier() {
        return new PercentileLongAggregatorFunctionSupplier(percentileValue(), tDigestStateCompression);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier() {
        return new PercentileIntAggregatorFunctionSupplier(percentileValue(), tDigestStateCompression);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier() {
        return new PercentileDoubleAggregatorFunctionSupplier(percentileValue(), tDigestStateCompression);
    }

    private double percentileValue() {
        return doubleValueOf(percentile(), source().text(), "Percentile");
    }

    @Override
    public Expression surrogate() {
        var field = field();
        DataType fieldType = field.dataType();

        if (fieldType == DataType.EXPONENTIAL_HISTOGRAM || fieldType == DataType.TDIGEST) {
            return new HistogramPercentile(source(), new HistogramMerge(source(), field, filter(), window()), percentile());
        }
        if (field.foldable()) {
            return new MvPercentile(source(), new ToDouble(source(), field), percentile());
        }

        return null;
    }
}
