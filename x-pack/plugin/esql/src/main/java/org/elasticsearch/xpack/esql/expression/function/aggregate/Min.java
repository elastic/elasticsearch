/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatial;

public class Min extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Min", Min::new);

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" },
        description = "The minimum value of a field.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "min"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the minimum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MIN` function",
                file = "stats",
                tag = "docsStatsMinNestedExpression"
            ) }
    )
    public Min(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Min(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Min(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Min> info() {
        return NodeInfo.create(this, Min::new, field(), filter());
    }

    @Override
    public Min replaceChildren(List<Expression> newChildren) {
        return new Min(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Min withFilter(Expression filter) {
        return new Min(source(), field(), filter);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            t -> isRepresentable(t) && t != UNSIGNED_LONG && isSpatial(t) == false,
            sourceText(),
            DEFAULT,
            "representable except unsigned_long and spatial types"
        );
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public final AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.BOOLEAN) {
            return new MinBooleanAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new MinLongAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.INTEGER) {
            return new MinIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DOUBLE) {
            return new MinDoubleAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.IP) {
            return new MinIpAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.VERSION || DataType.isString(type)) {
            return new MinBytesRefAggregatorFunctionSupplier(inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        return field().foldable() ? new MvMin(source(), field()) : null;
    }
}
