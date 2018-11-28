/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

/**
 * GROUP BY key for scripts (typically caused by functions).
 */
public class GroupByScriptKey extends GroupByKey {

    private final ScriptTemplate script;

    public GroupByScriptKey(String id, String fieldName, ScriptTemplate script) {
        this(id, fieldName, null, script);
    }

    public GroupByScriptKey(String id, String fieldName, Direction direction, ScriptTemplate script) {
        super(id, fieldName, direction);
        this.script = script;
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public TermsValuesSourceBuilder asValueSource() {
        TermsValuesSourceBuilder builder = new TermsValuesSourceBuilder(id())
                .script(script.toPainless())
                .order(direction().asOrder())
                .missingBucket(true);

        if (script.outputType().isInteger()) {
            builder.valueType(ValueType.LONG);
        } else if (script.outputType().isRational()) {
            builder.valueType(ValueType.DOUBLE);
        } else if (script.outputType().isString()) {
            builder.valueType(ValueType.STRING);
        } else if (script.outputType() == DataType.DATE) {
            builder.valueType(ValueType.DATE);
        } else if (script.outputType() == DataType.BOOLEAN) {
            builder.valueType(ValueType.BOOLEAN);
        } else if (script.outputType() == DataType.IP) {
            builder.valueType(ValueType.IP);
        }

        return builder;
    }

    @Override
    protected GroupByKey copy(String id, String fieldName, Direction direction) {
        return new GroupByScriptKey(id, fieldName, direction, script);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), script);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(script, ((GroupByScriptKey) obj).script);
    }
}
