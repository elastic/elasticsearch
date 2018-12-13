/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public abstract class RegexMatch extends BinaryPredicate<String, String, Boolean, RegexOperation> {

    protected RegexMatch(Location location, Expression value, Expression pattern) {
        super(location, value, pattern, RegexOperation.INSTANCE);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
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
