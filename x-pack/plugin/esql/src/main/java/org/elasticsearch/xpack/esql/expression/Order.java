/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

public class Order extends org.elasticsearch.xpack.esql.core.expression.Order {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Order", Order::new);

    public Order(Source source, Expression child, OrderDirection direction, NullsPosition nulls) {
        super(source, child, direction, nulls);
    }

    public Order(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readEnum(org.elasticsearch.xpack.esql.core.expression.Order.OrderDirection.class),
            in.readEnum(org.elasticsearch.xpack.esql.core.expression.Order.NullsPosition.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeEnum(direction());
        out.writeEnum(nullsPosition());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataType.isString(child().dataType())) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return super.resolveType();
    }

    @Override
    public Order replaceChildren(List<Expression> newChildren) {
        return new Order(source(), newChildren.get(0), direction(), nullsPosition());
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.esql.core.expression.Order> info() {
        return NodeInfo.create(this, Order::new, child(), direction(), nullsPosition());
    }
}
