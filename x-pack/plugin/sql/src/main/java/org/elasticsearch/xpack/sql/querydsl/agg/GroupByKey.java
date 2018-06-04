/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

import java.util.Objects;

/**
 * A key for a SQL GroupBy which maps to value source for composite aggregation.
 */
public abstract class GroupByKey extends Agg {

    private final Direction direction;

    GroupByKey(String id, String fieldName, Direction direction) {
        super(id, fieldName);
        // ASC is the default order of CompositeValueSource
        this.direction = direction == null ? Direction.ASC : direction;
    }

    public Direction direction() {
        return direction;
    }

    public abstract CompositeValuesSourceBuilder<?> asValueSource();

    protected abstract GroupByKey copy(String id, String fieldName, Direction direction);

    public GroupByKey with(Direction direction) {
        return this.direction == direction ? this : copy(id(), fieldName(), direction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id(), fieldName(), direction);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(direction, ((GroupByKey) obj).direction);
    }
}