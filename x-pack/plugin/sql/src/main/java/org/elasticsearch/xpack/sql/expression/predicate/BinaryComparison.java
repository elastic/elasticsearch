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

    /**
     * Compares two expression arguments (typically Numbers), if possible.
     * Otherwise returns null (the arguments are not comparable or at least
     * one of them is null).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Integer compare(Object l, Object r) {
        // typical number comparison
        if (l instanceof Number && r instanceof Number) {
            return compare((Number) l, (Number) r);
        }

        if (l instanceof Comparable && r instanceof Comparable) {
            try {
                return Integer.valueOf(((Comparable) l).compareTo(r));
            } catch (ClassCastException cce) {
                // when types are not compatible, cce is thrown
                // fall back to null
                return null;
            }
        }

        return null;
    }

    static Integer compare(Number l, Number r) {
        if (l instanceof Double || r instanceof Double) {
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.compare(l.floatValue(), r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.compare(l.longValue(), r.longValue());
        }

        return Integer.valueOf(Integer.compare(l.intValue(), r.intValue()));
    }
}
