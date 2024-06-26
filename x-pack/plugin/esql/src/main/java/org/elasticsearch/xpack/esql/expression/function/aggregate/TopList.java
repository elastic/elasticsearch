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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopListDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopListIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopListLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

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

public class TopList extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "TopList", TopList::new);

    private static final String ORDER_ASC = "ASC";
    private static final String ORDER_DESC = "DESC";

    @FunctionInfo(
        returnType = { "double", "integer", "long", "date" },
        description = "Collects the top values for a field. Includes repeated values.",
        isAggregation = true,
        examples = @Example(file = "stats_top_list", tag = "top-list")
    )
    public TopList(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "date" },
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

    private TopList(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readExpression(),
            ((PlanStreamInput) in).readExpression(),
            ((PlanStreamInput) in).readExpression()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        List<Expression> fields = children();
        assert fields.size() == 3;
        ((PlanStreamOutput) out).writeExpression(fields.get(0));
        ((PlanStreamOutput) out).writeExpression(fields.get(1));
        ((PlanStreamOutput) out).writeExpression(fields.get(2));
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
            dt -> dt == DataType.DATETIME || dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long or counter types"
        ).and(isFoldable(limitField(), sourceText(), SECOND))
            .and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"))
            .and(isFoldable(orderField(), sourceText(), THIRD))
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
        return field().dataType();
    }

    @Override
    protected NodeInfo<TopList> info() {
        return NodeInfo.create(this, TopList::new, children().get(0), children().get(1), children().get(2));
    }

    @Override
    public TopList replaceChildren(List<Expression> newChildren) {
        return new TopList(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new TopListLongAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.INTEGER) {
            return new TopListIntAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
        }
        if (type == DataType.DOUBLE) {
            return new TopListDoubleAggregatorFunctionSupplier(inputChannels, limitValue(), orderValue());
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
