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
import org.elasticsearch.compute.aggregation.*;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.*;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.*;

public class Last extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Last", Last::new);

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword" },
        description = "Returns the last reported value by date/time.",
        isAggregation = true,
        examples = @Example(file = "stats_first_last", tag = "last")
    )
    public Last(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text" },
            description = "The field to find the last value of."
        ) Expression field,
        @Param(
            name = "time_dimension",
            type = { "date" },
            description = "The time dimension."
        ) Expression time_dimension
    ) {
        this(source, field, Literal.TRUE, time_dimension);
    }

    public Last(Source source, Expression field, Expression filter, Expression time_dimension) {
        super(source, field, filter, asList(time_dimension));
    }

    private Last(StreamInput in) throws IOException {
        super(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readNamedWriteable(Expression.class) : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readNamedWriteableCollectionAsList(Expression.class)
                : asList(in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class))
        );
    }

    @Override
    protected void deprecatedWriteParams(StreamOutput out) throws IOException {
        List<? extends Expression> params = parameters();
        assert params.size() == 2;
        out.writeNamedWriteable(params.get(0));
        out.writeNamedWriteable(params.get(1));
    }

    @Override
    public Last withFilter(Expression filter) {
        return new Last(source(), field(), filter);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression timeDimension() {
        return parameters().get(0);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var typeResolution = isType(
            field(),
            dt -> dt == DataType.BOOLEAN
                || dt == DataType.DATETIME
                || dt == DataType.IP
                || DataType.isString(dt)
                // TODO — can we support unsigned long and counter types?
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
            sourceText(),
            FIRST,
            "boolean",
            "date",
            "ip",
            "string",
            "numeric except unsigned_long or counter types"
        ).and(isNotNullAndFoldable(timeDimension(), sourceText(), SECOND))
            .and(isType(timeDimension(), dt -> dt == DataType.DATETIME, sourceText(), SECOND, "date"));

        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<Last> info() {
        return NodeInfo.create(this, Last::new, field(), filter(), timeDimension());
    }

    @Override
    public Last replaceChildren(List<Expression> newChildren) {
        return new Last(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new TopLongAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        if (type == DataType.INTEGER) {
            return new TopIntAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        if (type == DataType.DOUBLE) {
            return new TopDoubleAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        if (type == DataType.BOOLEAN) {
            return new TopBooleanAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        if (type == DataType.IP) {
            return new TopIpAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        if (DataType.isString(type)) {
            return new TopBytesRefAggregatorFunctionSupplier(inputChannels, 1, true);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        return null;
    }
}
