/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing;

/**
 * GROUP BY key for constant values
 */
public final class GroupByConstant extends GroupByKey {

    public GroupByConstant(String id, String fieldName) {
        this(id, AggSource.of(fieldName), null, null);
    }

    private GroupByConstant(String id, AggSource source, Direction direction, Missing missing) {
        super(id, source, direction, missing);
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected GroupByKey copy(String id, AggSource source, Direction direction, Missing missing) {
        return new GroupByConstant(id, source(), direction, missing);
    }
}
