/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;

public abstract class BinaryLogic extends BinaryOperator<Boolean, Boolean, Boolean, BinaryLogicOperation> {

    protected BinaryLogic(Source source, Expression left, Expression right, BinaryLogicOperation operation) {
        super(source, left, right, operation);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal) {
        return isBoolean(e, sourceText(), paramOrdinal);
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryLogicPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), function());
    }

    @Override
    public Nullability nullable() {
        // Cannot fold null due to 3vl, constant folding will do any possible folding.
        return Nullability.UNKNOWN;
    }
}
