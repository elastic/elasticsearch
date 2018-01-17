/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

public class Order extends UnaryExpression {

    public enum OrderDirection {
        ASC, DESC
    }

    private final OrderDirection direction;

    public Order(Location location, Expression child, OrderDirection direction) {
        super(location, child);
        this.direction = direction;
    }

    @Override
    protected NodeInfo<Order> info() {
        return NodeInfo.create(this, Order::new, child(), direction);
    }

    @Override
    protected UnaryExpression replaceChild(Expression newChild) {
        return new Order(location(), newChild, direction);
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
        return Objects.hash(child(), direction);
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
                && Objects.equals(child(), other.child());
    }
}
