/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.container;

import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;

public abstract class Sort {

    public enum Direction {
        ASC, DESC;

        public static Direction from(OrderDirection dir) {
            return dir == null || dir == OrderDirection.ASC ? ASC : DESC;
        }

        public SortOrder asOrder() {
            return this == Direction.ASC ? SortOrder.ASC : SortOrder.DESC;
        }
    }

    public enum Missing {
        FIRST("_first"), LAST("_last");

        private final String position;

        Missing(String position) {
            this.position = position;
        }

        public static Missing from(NullsPosition pos) {
            return pos == null || pos == NullsPosition.FIRST ? FIRST : LAST;
        }

        public String position() {
            return position;
        }
    }

    private final Direction direction;
    private final Missing missing;

    protected Sort(Direction direction, Missing nulls) {
        this.direction = direction;
        this.missing = nulls;
    }

    public Direction direction() {
        return direction;
    }

    public Missing missing() {
        return missing;
    }
}
