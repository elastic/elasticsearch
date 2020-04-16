/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;

/**
 * GROUP BY key for fields or scripts.
 */
public class GroupByValue extends GroupByKey {

    public GroupByValue(String id, String fieldName) {
        this(id, AggTarget.of(fieldName), null);
    }

    public GroupByValue(String id, ScriptTemplate script) {
        this(id, AggTarget.of(script), null);
    }

    private GroupByValue(String id, AggTarget target, Direction direction) {
        super(id, target, direction);
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        return new TermsValuesSourceBuilder(id());
    }

    @Override
    protected GroupByKey copy(String id, AggTarget target, Direction direction) {
        return new GroupByValue(id, target(), direction);
    }
}
