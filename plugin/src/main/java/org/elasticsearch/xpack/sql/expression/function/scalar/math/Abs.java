/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnsProcessor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public class Abs extends MathFunction {

    public Abs(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    public ColumnsProcessor asProcessor() {
        return l -> {
            if (l instanceof Float) {
                return Math.abs(((Float) l).floatValue());
            }
            if (l instanceof Double) {
                return Math.abs(((Double) l).doubleValue());
            }
            long lo = ((Number) l).longValue();
            return lo >= 0 ? lo : lo == Long.MIN_VALUE ? Long.MAX_VALUE : -lo;
        };
    }

    @Override
    public DataType dataType() {
        return argument().dataType();
    }

    @Override
    protected Object math(double d) {
        throw new UnsupportedOperationException("unused");
    }
}
