/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

/**
 * GROUP BY key for regular fields.
 */
public class GroupByColumnKey extends GroupByKey {

    public GroupByColumnKey(String id, String fieldName) {
        this(id, fieldName, null);
    }

    public GroupByColumnKey(String id, String fieldName, Direction direction) {
        super(id, fieldName, direction);
    }

    @Override
    public TermsValuesSourceBuilder asValueSource() {
        return new TermsValuesSourceBuilder(id())
                .field(fieldName())
                .order(direction().asOrder())
                .missingBucket(true);
    }

    @Override
    protected GroupByKey copy(String id, String fieldName, Direction direction) {
        return new GroupByColumnKey(id, fieldName, direction);
    }
}
