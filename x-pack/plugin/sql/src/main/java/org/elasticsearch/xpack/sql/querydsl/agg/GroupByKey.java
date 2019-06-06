/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

/**
 * A key for a SQL GroupBy which maps to value source for composite aggregation.
 */
public abstract class GroupByKey extends Agg {

    protected final Direction direction;
    private final ScriptTemplate script;

    protected GroupByKey(String id, String fieldName, ScriptTemplate script, Direction direction) {
        super(id, fieldName);
        // ASC is the default order of CompositeValueSource
        this.direction = direction == null ? Direction.ASC : direction;
        this.script = script;
    }

    public final CompositeValuesSourceBuilder<?> asValueSource() {
        CompositeValuesSourceBuilder<?> builder = createSourceBuilder();
        
        if (script != null) {
            builder.script(script.toPainless());
            if (script.outputType().isInteger()) {
                builder.valueType(ValueType.LONG);
            } else if (script.outputType().isRational()) {
                builder.valueType(ValueType.DOUBLE);
            } else if (script.outputType().isString()) {
                builder.valueType(ValueType.STRING);
            } else if (script.outputType() == DataType.DATE) {
                builder.valueType(ValueType.LONG);
            } else if (script.outputType() == DataType.TIME) {
                builder.valueType(ValueType.LONG);
            } else if (script.outputType() == DataType.DATETIME) {
                builder.valueType(ValueType.LONG);
            } else if (script.outputType() == DataType.BOOLEAN) {
                builder.valueType(ValueType.BOOLEAN);
            } else if (script.outputType() == DataType.IP) {
                builder.valueType(ValueType.IP);
            }
        }
        // field based
        else {
            builder.field(fieldName());
        }
        return builder.order(direction.asOrder())
               .missingBucket(true);
    }

    protected abstract CompositeValuesSourceBuilder<?> createSourceBuilder();

    protected abstract GroupByKey copy(String id, String fieldName, ScriptTemplate script, Direction direction);

    public GroupByKey with(Direction direction) {
        return this.direction == direction ? this : copy(id(), fieldName(), script, direction);
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id(), fieldName(), script, direction);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && Objects.equals(script, ((GroupByKey) obj).script)
                && Objects.equals(direction, ((GroupByKey) obj).direction);
    }
}
