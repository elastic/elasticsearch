/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorMatch;

public class VectorBinarySet extends VectorBinaryOperator {

    public enum SetOp implements BinaryOp {
        INTERSECT,
        SUBTRACT,
        UNION;

        @Override
        public ScalarFunctionFactory asFunction() {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    private final SetOp op;

    public VectorBinarySet(Source source, Expression left, Expression right, VectorMatch match, SetOp op) {
        super(source, left, right, match, true, op);
        this.op = op;
    }

    public SetOp op() {
        return op;
    }

    @Override
    protected VectorBinarySet replaceChildren(Expression left, Expression right) {
        return new VectorBinarySet(source(), left, right, match(), op());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, VectorBinarySet::new, left(), right(), match(), op());
    }
}
