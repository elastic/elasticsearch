/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopValuesListLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSort;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class TopValuesList extends AggregateFunction implements ToAggregator, SurrogateExpression {
    private static final String ORDER_ASC = "ASC";
    private static final String ORDER_DESC = "DESC";

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
        description = "Collects the top values for a field.",
        isAggregation = true
    )
    public TopValuesList(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "The field to collect the top values for."
        ) Expression field,
        @Param(name = "limit", type = { "integer" }, description = "The maximum number of values to collect.") Expression limit,
        @Param(
            name = "order",
            type = { "keyword" },
            description = "The order to calculate the top values. Either `asc` or `desc`."
        ) Expression order
    ) {
        super(source, field, Arrays.asList(limit, order));
    }

    public static TopValuesList readFrom(PlanStreamInput in) throws IOException {
        return new TopValuesList(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readExpression());
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        source().writeTo(out);
        List<Expression> fields = children();
        assert fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeExpression(fields.get(2));
    }

    private Expression limitField() {
        return parameters().get(0);
    }

    private Expression orderField() {
        return parameters().get(1);
    }

    private int limitValue() {
        return (int) limitField().fold();
    }

    private boolean orderValue() {
        return orderField().fold().toString().equalsIgnoreCase(ORDER_ASC);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var typeResolution = isType(field(), EsqlDataTypes::isRepresentable, sourceText(), FIRST, "representable").and(
            isFoldable(limitField(), sourceText(), SECOND)
        )
            .and(isType(limitField(), dt -> dt == DataTypes.INTEGER, sourceText(), SECOND, "integer"))
            .and(isFoldable(orderField(), sourceText(), THIRD))
            .and(isString(orderField(), sourceText(), THIRD));

        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        var limit = limitValue();
        var order = orderField().fold().toString();

        if (limit <= 0) {
            return new TypeResolution(format(null, "Limit must be greater than 0. Got {}", limit));
        }

        if (order.equalsIgnoreCase(ORDER_ASC) == false && order.equalsIgnoreCase(ORDER_DESC) == false) {
            return new TypeResolution(format(null, "Invalid order value. Expected [{}, {}] but got {}", ORDER_ASC, ORDER_DESC, order));
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<TopValuesList> info() {
        return NodeInfo.create(this, TopValuesList::new, children().get(0), children().get(1), children().get(2));
    }

    @Override
    public TopValuesList replaceChildren(List<Expression> newChildren) {
        return new TopValuesList(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataTypes.LONG || type == DataTypes.DATETIME) {
            return new TopValuesListLongAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        var s = source();

        if (field().dataType() == DataTypes.LONG) {
            return null;
        }

        // Base, unoptimized implementation
        // TODO: VALUES() doesn't keep duplicates! So we can't surrogate this for foldable expressions
        return new MvSlice(
            s,
            new MvSort(s, new Values(s, field()), orderField()),
            new Literal(s, 0, DataTypes.INTEGER),
            new Literal(s, limitValue() - 1, DataTypes.INTEGER)
        );
    }
}
