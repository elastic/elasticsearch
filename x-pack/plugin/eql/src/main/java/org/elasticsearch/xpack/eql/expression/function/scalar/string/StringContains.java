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

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor.doProcess;

/**
 * EQL specific stringContains function.
 * stringContains(a, b)
 * Returns true if b is a substring of a
 */
public class StringContains extends BinaryComparisonCaseInsensitiveFunction {

    public StringContains(Source source, Expression string, Expression substring, boolean caseInsensitive) {
        super(source, string, substring, caseInsensitive);
    }

    public Expression string() {
        return left();
    }

    public Expression substring() {
        return right();
    }

    @Override
    protected Pipe makePipe() {
        return new StringContainsFunctionPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), isCaseInsensitive());
    }

    @Override
    public Object fold() {
        return doProcess(left().fold(), right().fold(), isCaseInsensitive());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StringContains::new, left(), right(), isCaseInsensitive());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StringContains(source(), newChildren.get(0), newChildren.get(1), isCaseInsensitive());
    }
}
