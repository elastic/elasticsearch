/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class Order extends Expression {

    public enum OrderDirection {
        ASC, DESC
    }

    private final Expression child;
    private final OrderDirection direction;

    public Order(Location location, Expression child, OrderDirection direction) {
        super(location, singletonList(child));
        this.child = child;
        this.direction = direction;
    }

    @Override
    protected NodeInfo<Order> info() {
        return NodeInfo.create(this, Order::new, child, direction);
    }

    @Override
    public boolean nullable() {
        return false;
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
        return new Order(location(), newChildren.get(0), direction);
    }

    public Expression child() {
        return child;
    }

    public OrderDirection direction() {
        return direction;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child, direction);
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
                && Objects.equals(child, other.child);
    }
}