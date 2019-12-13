/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

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
