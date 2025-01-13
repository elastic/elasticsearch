/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentile;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Percentile extends NumericAggregate implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Percentile",
        Percentile::new
    );

    private final Expression percentile;

    @FunctionInfo(
        returnType = "double",
        description = "Returns the value at which a certain percentage of observed values occur. "
            + "For example, the 95th percentile is the value which is greater than 95% of the "
            + "observed values and the 50th percentile is the `MEDIAN`.",
        appendix = """
            [discrete]
            [[esql-percentile-approximate]]
            ==== `PERCENTILE` is (usually) approximate

            include::../../../aggregations/metrics/percentile-aggregation.asciidoc[tag=approximate]

            [WARNING]
            ====
            `PERCENTILE` is also {wikipedia}/Nondeterministic_algorithm[non-deterministic].
            This means you can get slightly different results using the same data.
            ====
            """,
        isAggregation = true,
        examples = {
            @Example(file = "stats_percentile", tag = "percentile"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate a percentile "
                    + "of the maximum values of a multivalued column, first use `MV_MAX` to get the "
                    + "maximum value per row, and use the result with the `PERCENTILE` function",
                file = "stats_percentile",
                tag = "docsStatsPercentileNestedExpression"
            ), }
    )
    public Percentile(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }) Expression field,
        @Param(name = "percentile", type = { "double", "integer", "long" }) Expression percentile
    ) {
        this(source, field, Literal.TRUE, percentile);
    }

    public Percentile(Source source, Expression field, Expression filter, Expression percentile) {
        super(source, field, filter, singletonList(percentile));
        this.percentile = percentile;
    }

    private Percentile(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readNamedWriteable(Expression.class) : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readNamedWriteableCollectionAsList(Expression.class).get(0)
                : in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    protected void deprecatedWriteParams(StreamOutput out) throws IOException {
        out.writeNamedWriteable(percentile);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), filter(), percentile);
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        return new Percentile(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Percentile withFilter(Expression filter) {
        return new Percentile(source(), field(), filter, percentile);
    }

    public Expression percentile() {
        return percentile;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long"
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
        ).and(isFoldable(percentile, sourceText(), SECOND));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        return new PercentileIntAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        return new PercentileDoubleAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    private int percentileValue() {
        return ((Number) percentile.fold()).intValue();
    }

    @Override
    public Expression surrogate() {
        var field = field();

        if (field.foldable()) {
            return new MvPercentile(source(), new ToDouble(source(), field), percentile());
        }

        return null;
    }
}
