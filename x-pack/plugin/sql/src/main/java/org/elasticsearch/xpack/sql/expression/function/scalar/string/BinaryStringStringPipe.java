/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;

import java.util.Objects;

/**
 * String operations pipe requiring two string arguments.
 */
public class BinaryStringStringPipe extends BinaryPipe {

    private final BinaryStringStringOperation operation;

    public BinaryStringStringPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryStringStringOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryStringStringPipe> info() {
        return NodeInfo.create(this, BinaryStringStringPipe::new, expression(), left(), right(), operation);
    }

    public BinaryStringStringOperation operation() {
        return operation;
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryStringStringPipe(source(), expression(), left, right, operation);
    }

    @Override
    public BinaryStringStringProcessor asProcessor() {
        return new BinaryStringStringProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinaryStringStringPipe other = (BinaryStringStringPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
