/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.sql.expression.Order.OrderDirection;

public class Sort {

    public enum Direction {
        ASC, DESC;

        public static Direction from(OrderDirection dir) {
            return dir == null || dir == OrderDirection.ASC ? ASC : DESC;
        }

        public SortOrder asOrder() {
            return this == Direction.ASC ? SortOrder.ASC : SortOrder.DESC;
        }
    }

    private final Direction direction;

    protected Sort(Direction direction) {
        this.direction = direction;
    }

    public Direction direction() {
        return direction;
    }
}
