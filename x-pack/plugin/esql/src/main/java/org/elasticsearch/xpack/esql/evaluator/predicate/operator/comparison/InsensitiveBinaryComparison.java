/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

public abstract class InsensitiveBinaryComparison extends BinaryScalarFunction {

    private final ZoneId zoneId;

    protected InsensitiveBinaryComparison(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right);
        this.zoneId = zoneId;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    /**
     * Reverses the direction of this comparison on the comparison axis.
     * Some operations like Greater/LessThan/OrEqual will behave as if the operands of a numerical comparison get multiplied with a
     * negative number. Others like Not/Equal can be immutable to this operation.
     */
    public abstract InsensitiveBinaryComparison reverse();
}
