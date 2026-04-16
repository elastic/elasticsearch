/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneId;

// marker class to indicate operations that rely on values
public abstract class BinaryComparison extends BinaryOperator<Object, Object, Boolean, BinaryComparisonOperation> {

    private final ZoneId zoneId;

    protected BinaryComparison(Source source, Expression left, Expression right, BinaryComparisonOperation operation, ZoneId zoneId) {
        super(source, left, right, operation);
        this.zoneId = zoneId;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, ParamOrdinal paramOrdinal) {
        return TypeResolutions.isExact(e, sourceText(), paramOrdinal);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    public static Integer compare(Object left, Object right) {
        return Comparisons.compare(left, right);
    }

    /**
     * Reverses the direction of this comparison on the comparison axis.
     * Some operations like Greater/LessThan/OrEqual will behave as if the operands of a numerical comparison get multiplied with a
     * negative number. Others like Not/Equal can be immutable to this operation.
     */
    public abstract BinaryComparison reverse();
}
