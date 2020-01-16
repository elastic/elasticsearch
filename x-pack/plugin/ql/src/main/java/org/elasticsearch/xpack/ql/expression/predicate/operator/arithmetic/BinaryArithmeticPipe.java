/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class BinaryArithmeticPipe extends BinaryPipe {

    private final BinaryArithmeticOperation operation;

    public BinaryArithmeticPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryArithmeticOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryArithmeticPipe> info() {
        return NodeInfo.create(this, BinaryArithmeticPipe::new, expression(), left(), right(), operation);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryArithmeticPipe(source(), expression(), left, right, operation);
    }

    @Override
    public BinaryArithmeticProcessor asProcessor() {
        return new BinaryArithmeticProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinaryArithmeticPipe other = (BinaryArithmeticPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
