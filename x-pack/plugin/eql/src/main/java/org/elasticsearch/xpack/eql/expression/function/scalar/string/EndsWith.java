/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.BinaryComparisonCaseInsensitiveFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWithFunctionProcessor.doProcess;

/**
 * Function that checks if first parameter ends with the second parameter. Both parameters should be strings
 * and the function returns a boolean value. The function is case insensitive.
 */
public class EndsWith extends BinaryComparisonCaseInsensitiveFunction {

    public EndsWith(Source source, Expression input, Expression pattern, boolean caseInsensitive) {
        super(source, input, pattern, caseInsensitive);
    }

    public Expression input() {
        return left();
    }

    public Expression pattern() {
        return right();
    }

    @Override
    protected Pipe makePipe() {
        return new EndsWithFunctionPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), isCaseInsensitive());
    }

    @Override
    public Object fold() {
        return doProcess(left().fold(), right().fold(), isCaseInsensitive());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EndsWith::new, left(), right(), isCaseInsensitive());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new EndsWith(source(), newChildren.get(0), newChildren.get(1), isCaseInsensitive());
    }
}
