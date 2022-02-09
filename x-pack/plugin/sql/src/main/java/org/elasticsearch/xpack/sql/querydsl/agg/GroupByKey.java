/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Objects;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;

/**
 * A key for a SQL GroupBy which maps to value source for composite aggregation.
 */
public abstract class GroupByKey extends Agg {

    protected final Direction direction;
    private final Missing missing;

    protected GroupByKey(String id, AggSource source, Direction direction, Missing missing) {
        super(id, source);
        // ASC is the default order of CompositeValueSource
        this.direction = direction == null ? Direction.ASC : direction;
        this.missing = missing == null ? Missing.ANY : missing;
    }

    public ScriptTemplate script() {
        return source().script();
    }

    public final CompositeValuesSourceBuilder<?> asValueSource() {
        CompositeValuesSourceBuilder<?> builder = createSourceBuilder();
        ScriptTemplate script = source().script();
        if (script != null) {
            builder.script(script.toPainless());
            if (script.outputType().isInteger()) {
                builder.userValuetypeHint(ValueType.LONG);
            } else if (script.outputType().isRational()) {
                builder.userValuetypeHint(ValueType.DOUBLE);
            } else if (DataTypes.isString(script.outputType())) {
                builder.userValuetypeHint(ValueType.STRING);
            } else if (script.outputType() == DATE) {
                builder.userValuetypeHint(ValueType.LONG);
            } else if (script.outputType() == TIME) {
                builder.userValuetypeHint(ValueType.LONG);
            } else if (script.outputType() == DATETIME) {
                builder.userValuetypeHint(ValueType.LONG);
            } else if (script.outputType() == BOOLEAN) {
                builder.userValuetypeHint(ValueType.BOOLEAN);
            } else if (script.outputType() == IP) {
                builder.userValuetypeHint(ValueType.IP);
            }
        }
        // field based
        else {
            builder.field(source().fieldName());
        }
        builder.order(direction.asOrder()).missingBucket(true);

        if (missing.aggregationOrder() != null) {
            builder.missingOrder(missing.aggregationOrder());
        }

        return builder;
    }

    protected abstract CompositeValuesSourceBuilder<?> createSourceBuilder();

    protected abstract GroupByKey copy(String id, AggSource source, Direction direction, Missing missing);

    public GroupByKey with(Direction direction, Missing missing) {
        return this.direction == direction && this.missing == missing ? this : copy(id(), source(), direction, missing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), direction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        GroupByKey that = (GroupByKey) o;
        return direction == that.direction;
    }
}
