/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryOptionalMathProcessor.BinaryOptionalMathOperation;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class BinaryOptionalMathPipe extends Pipe {

    private final Pipe left, right;
    private final BinaryOptionalMathOperation operation;

    public BinaryOptionalMathPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryOptionalMathOperation operation) {
        super(source, expression, right == null ? Arrays.asList(left) : Arrays.asList(left, right));
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        int childrenSize = newChildren.size();
        if (childrenSize > 2 || childrenSize < 1) {
            throw new IllegalArgumentException("expected [1 or 2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), childrenSize == 1 ? null : newChildren.get(1));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newLeft = left.resolveAttributes(resolver);
        Pipe newRight = right == null ? right : right.resolveAttributes(resolver);
        if (newLeft == left && newRight == right) {
            return this;
        }
        return replaceChildren(newLeft, newRight);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return right == null ? left.supportedByAggsOnlyQuery() : left.supportedByAggsOnlyQuery() || right.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return left.resolved() && (right == null || right.resolved());
    }

    protected Pipe replaceChildren(Pipe newLeft, Pipe newRight) {
        return new BinaryOptionalMathPipe(source(), expression(), newLeft, newRight, operation);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        left.collectFields(sourceBuilder);
        if (right != null) {
            right.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<BinaryOptionalMathPipe> info() {
        return NodeInfo.create(this, BinaryOptionalMathPipe::new, expression(), left, right, operation);
    }

    @Override
    public BinaryOptionalMathProcessor asProcessor() {
        return new BinaryOptionalMathProcessor(left.asProcessor(), right == null ? null : right.asProcessor(), operation);
    }
    
    public Pipe right() {
        return right;
    }
    
    public Pipe left() {
        return left;
    }
    
    public BinaryOptionalMathOperation operation() {
        return operation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryOptionalMathPipe other = (BinaryOptionalMathPipe) obj;
        return Objects.equals(left, other.left) 
                && Objects.equals(right, other.right)
                && Objects.equals(operation, other.operation);
    }
}
