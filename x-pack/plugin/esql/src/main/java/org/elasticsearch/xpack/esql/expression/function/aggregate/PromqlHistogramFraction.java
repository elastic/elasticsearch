/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PromqlHistogramFractionAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.doubleValueOf;

/** Internal aggregate implementing Prometheus classic-histogram fraction evaluation. */
public class PromqlHistogramFraction extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PromqlHistogramFraction",
        PromqlHistogramFraction::readFrom
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "double",
        type = FunctionType.AGGREGATE
    )
    public PromqlHistogramFraction(
        Source source,
        @Param(name = "count", type = { "double" }) Expression field,
        @Param(name = "upper_bound", type = { "keyword" }) Expression upperBound,
        @Param(name = "lower", type = { "double", "integer", "long" }) Expression lower,
        @Param(name = "upper", type = { "double", "integer", "long" }) Expression upper
    ) {
        this(source, field, upperBound, Literal.TRUE, NO_WINDOW, lower, upper);
    }

    public PromqlHistogramFraction(
        Source source,
        Expression field,
        Expression upperBound,
        Expression filter,
        Expression window,
        Expression lower,
        Expression upper
    ) {
        super(source, List.of(field, upperBound), filter, window, List.of(lower, upper));
    }

    private static PromqlHistogramFraction readFrom(StreamInput in) throws IOException {
        // Legacy serialization format for backwards compatibility.
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> rest = in.readNamedWriteableCollectionAsList(Expression.class);
        return new PromqlHistogramFraction(source, field, rest.get(0), filter, window, rest.get(1), rest.get(2));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility.
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(List.of(upperBound(), lower(), upper()));
    }

    public Expression field() {
        return fields().get(0);
    }

    public Expression upperBound() {
        return fields().get(1);
    }

    public Expression lower() {
        return parameters().get(0);
    }

    public Expression upper() {
        return parameters().get(1);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.DOUBLE, sourceText(), FIRST, "double").and(
            isType(upperBound(), dt -> dt == DataType.KEYWORD, sourceText(), SECOND, "keyword")
        )
            .and(isType(lower(), PromqlHistogramFraction::isSupportedBoundType, sourceText(), THIRD, "numeric except unsigned_long"))
            .and(isFoldable(lower(), sourceText(), THIRD))
            .and(isNotNull(lower(), sourceText(), THIRD))
            .and(isType(upper(), PromqlHistogramFraction::isSupportedBoundType, sourceText(), FOURTH, "numeric except unsigned_long"))
            .and(isFoldable(upper(), sourceText(), FOURTH))
            .and(isNotNull(upper(), sourceText(), FOURTH));
    }

    private static boolean isSupportedBoundType(DataType dataType) {
        return dataType.isNumeric() && dataType != DataType.UNSIGNED_LONG;
    }

    @Override
    protected NodeInfo<PromqlHistogramFraction> info() {
        return NodeInfo.create(this, PromqlHistogramFraction::new, field(), upperBound(), filter(), window(), lower(), upper());
    }

    @Override
    public PromqlHistogramFraction replaceChildren(List<Expression> newChildren) {
        return new PromqlHistogramFraction(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.get(5)
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new PromqlHistogramFractionAggregatorFunctionSupplier(source(), boundValue(lower()), boundValue(upper()));
    }

    private double boundValue(Expression bound) {
        return doubleValueOf(bound, source().text(), "PromqlHistogramFraction");
    }
}
