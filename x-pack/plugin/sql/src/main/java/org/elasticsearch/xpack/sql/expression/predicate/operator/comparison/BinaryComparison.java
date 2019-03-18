/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.TypeResolutions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

// marker class to indicate operations that rely on values
public abstract class BinaryComparison extends BinaryOperator<Object, Object, Boolean, BinaryComparisonOperation> {

    protected BinaryComparison(Source source, Expression left, Expression right, BinaryComparisonOperation operation) {
        super(source, left, right, operation);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isExact(e, sourceText(), paramOrdinal);
    }

    @Override
    protected Expression canonicalize() {
        return left().hashCode() > right().hashCode() ? swapLeftAndRight() : this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryComparisonPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), function());
    }

    public static Integer compare(Object left, Object right) {
        return Comparisons.compare(left, right);
    }
}
