/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

// marker class to indicate operations that rely on values
public abstract class BinaryComparison extends BinaryOperator {

    public BinaryComparison(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    protected TypeResolution resolveInputType(DataType inputType) {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected Expression canonicalize() {
        return left().hashCode() > right().hashCode() ? swapLeftAndRight() : this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Integer compare(Object left, Object right) {
        if (left instanceof Comparable && right instanceof Comparable) {
            return Integer.valueOf(((Comparable) left).compareTo(right));
        }
        return null;
    }
}
