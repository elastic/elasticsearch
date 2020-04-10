/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class BinaryLogicPipe extends BinaryPipe {

    private final BinaryLogicOperation operation;

    public BinaryLogicPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryLogicOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryLogicPipe> info() {
        return NodeInfo.create(this, BinaryLogicPipe::new, expression(), left(), right(), operation);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryLogicPipe(source(), expression(), left, right, operation);
    }

    @Override
    public BinaryLogicProcessor asProcessor() {
        return new BinaryLogicProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinaryLogicPipe other = (BinaryLogicPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
