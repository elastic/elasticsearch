/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;

import java.util.function.BiFunction;

/**
 * Returns the position of the first character expression in the second character expression, if not found it returns 0.
 */
public class Position extends BinaryStringStringFunction {

    public Position(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected BiFunction<String, String, Number> operation() {
        return BinaryStringStringOperation.POSITION;
    }

    @Override
    protected Position replaceChildren(Expression newLeft, Expression newRight) {
        return new Position(source(), newLeft, newRight);
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryStringStringPipe(source(), this,
                Expressions.pipe(left()),
                Expressions.pipe(right()),
                BinaryStringStringOperation.POSITION);
    }

    @Override
    protected NodeInfo<Position> info() {
        return NodeInfo.create(this, Position::new, left(), right());
    }
}
