/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.container;

import org.elasticsearch.search.aggregations.bucket.composite.MissingOrder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;

public abstract class Sort {

    public enum Direction {
        ASC,
        DESC;

        public static Direction from(OrderDirection dir) {
            return dir == null || dir == OrderDirection.ASC ? ASC : DESC;
        }

        public SortOrder asOrder() {
            return this == Direction.ASC ? SortOrder.ASC : SortOrder.DESC;
        }
    }

    public enum Missing {
        FIRST("_first", MissingOrder.FIRST),
        LAST("_last", MissingOrder.LAST),
        /**
         * Nulls position has not been specified by the user and an appropriate default will be used.
         *
         * The default values are chosen such that it stays compatible with previous behavior. Unfortunately, this results in
         * inconsistencies across different types of queries (see https://github.com/elastic/elasticsearch/issues/77068).
         */
        ANY(null, null);

        private final String searchOrder;
        private final MissingOrder aggregationOrder;

        Missing(String searchOrder, MissingOrder aggregationOrder) {
            this.searchOrder = searchOrder;
            this.aggregationOrder = aggregationOrder;
        }

        public static Missing from(NullsPosition pos) {
            return switch (pos) {
                case FIRST -> FIRST;
                case LAST -> LAST;
                default -> ANY;
            };
        }

        public String searchOrder() {
            return searchOrder(null);
        }

        /**
         * Preferred order of null values in non-aggregation queries.
         */
        public String searchOrder(Direction fallbackDirection) {
            if (searchOrder != null) {
                return searchOrder;
            } else {
                return switch (fallbackDirection) {
                    case ASC -> LAST.searchOrder;
                    case DESC -> FIRST.searchOrder;
                };
            }
        }

        /**
         * Preferred order of null values in aggregation queries.
         */
        public MissingOrder aggregationOrder() {
            return aggregationOrder;
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
