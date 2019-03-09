/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isExact;

public class Order extends Expression {

    public enum OrderDirection {
        ASC, DESC
    }

    public enum NullsPosition {
        FIRST, LAST;
    }

    private final Expression child;
    private final OrderDirection direction;
    private final NullsPosition nulls;

    public Order(Source source, Expression child, OrderDirection direction, NullsPosition nulls) {
        super(source, singletonList(child));
        this.child = child;
        this.direction = direction;
        this.nulls = nulls == null ? (direction == OrderDirection.DESC ? NullsPosition.FIRST : NullsPosition.LAST) : nulls;
    }

    @Override
    protected NodeInfo<Order> info() {
        return NodeInfo.create(this, Order::new, child, direction, nulls);
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isExact(child, "ORDER BY cannot be applied to field of data type [{}]: {}");
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Order replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Order(source(), newChildren.get(0), direction, nulls);
    }

    public Expression child() {
        return child;
    }

    public OrderDirection direction() {
        return direction;
    }

    public NullsPosition nullsPosition() {
        return nulls;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child, direction, nulls);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Order other = (Order) obj;
        return Objects.equals(direction, other.direction)
                && Objects.equals(nulls, other.nulls)
                && Objects.equals(child, other.child);
    }
}
