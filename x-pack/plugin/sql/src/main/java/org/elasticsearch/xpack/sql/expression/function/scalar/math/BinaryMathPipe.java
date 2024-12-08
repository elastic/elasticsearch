/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;

import java.util.Objects;

/**
 * Math operation pipe requiring two arguments.
 */
public class BinaryMathPipe extends BinaryPipe {

    private final BinaryMathOperation operation;

    public BinaryMathPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryMathOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryMathPipe> info() {
        return NodeInfo.create(this, BinaryMathPipe::new, expression(), left(), right(), operation);
    }

    public BinaryMathOperation operation() {
        return operation;
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryMathPipe(source(), expression(), left, right, operation);
    }

    @Override
    public BinaryMathProcessor asProcessor() {
        return new BinaryMathProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinaryMathPipe other = (BinaryMathPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
