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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongAggregatorFunctionSupplier;
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
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Top extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Top", Top::new);

    private static final String ORDER_ASC = "ASC";
    private static final String ORDER_DESC = "DESC";

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword" },
        description = "Collects the top values for a field. Includes repeated values.",
        isAggregation = true,
        examples = @Example(file = "stats_top", tag = "top")
    )
    public Top(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text" },
            description = "The field to collect the top values for."
        ) Expression field,
        @Param(name = "limit", type = { "integer" }, description = "The maximum number of values to collect.") Expression limit,
        @Param(
            name = "order",
            type = { "keyword" },
            description = "The order to calculate the top values. Either `asc` or `desc`."
        ) Expression order
    ) {
        this(source, field, Literal.TRUE, limit, order);
    }

    public Top(Source source, Expression field, Expression filter, Expression limit, Expression order) {
        super(source, field, filter, asList(limit, order));
    }

    private Top(StreamInput in) throws IOException {
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
    public Top withFilter(Expression filter) {
        return new Top(source(), field(), filter, limitField(), orderField());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression limitField() {
        return parameters().get(0);
    }

    Expression orderField() {
        return parameters().get(1);
    }

    private int limitValue() {
        return (int) limitField().fold();
    }

    private String orderRawValue() {
        return BytesRefs.toString(orderField().fold());
    }

    private boolean orderValue() {
        return orderRawValue().equalsIgnoreCase(ORDER_ASC);
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
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
            sourceText(),
            FIRST,
            "boolean",
            "date",
            "ip",
            "string",
            "numeric except unsigned_long or counter types"
        ).and(isNotNullAndFoldable(limitField(), sourceText(), SECOND))
            .and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"))
            .and(isNotNullAndFoldable(orderField(), sourceText(), THIRD))
            .and(isString(orderField(), sourceText(), THIRD));

        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        var limit = limitValue();
        var order = orderRawValue();

        if (limit <= 0) {
            return new TypeResolution(format(null, "Limit must be greater than 0 in [{}], found [{}]", sourceText(), limit));
        }

        if (order.equalsIgnoreCase(ORDER_ASC) == false && order.equalsIgnoreCase(ORDER_DESC) == false) {
            return new TypeResolution(
                format(null, "Invalid order value in [{}], expected [{}, {}] but got [{}]", sourceText(), ORDER_ASC, ORDER_DESC, order)
            );
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<Top> info() {
        return NodeInfo.create(this, Top::new, field(), filter(), limitField(), orderField());
    }

    @Override
    public Top replaceChildren(List<Expression> newChildren) {
        return new Top(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new TopLongAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.INTEGER) {
            return new TopIntAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.DOUBLE) {
            return new TopDoubleAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.BOOLEAN) {
            return new TopBooleanAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.IP) {
            return new TopIpAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (DataType.isString(type)) {
            return new TopBytesRefAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        var s = source();

        if (limitValue() == 1) {
            if (orderValue()) {
                return new Min(s, field());
            } else {
                return new Max(s, field());
            }
        }

        return null;
    }
}
