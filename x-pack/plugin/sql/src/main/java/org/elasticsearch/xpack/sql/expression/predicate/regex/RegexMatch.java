/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public abstract class RegexMatch extends BinaryPredicate<String, String, Boolean, RegexOperation> {

    private final Expression field;
    private final Expression pattern;

    protected RegexMatch(Location location, Expression value, Expression pattern) {
        super(location, value, pattern, RegexOperation.INSTANCE);
        this.field = value;
        this.pattern = pattern;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        if (pattern == null) {
            return Nullability.TRUE;
        }
        return field.nullable();
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return left().foldable();
    }

    @Override
    public Boolean fold() {
        Object val = left().fold();
        val = val != null ? val.toString() : val;
        return function().apply((String) val, asString(right()));
    }

    protected abstract String asString(Expression pattern);
}
