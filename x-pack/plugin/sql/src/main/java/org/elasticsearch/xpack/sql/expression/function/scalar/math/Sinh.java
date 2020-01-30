/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

/**
 * <a href="https://en.wikipedia.org/wiki/Hyperbolic_function">Hyperbolic sine</a>
 * function.
 */
public class Sinh extends MathFunction {
    public Sinh(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sinh> info() {
        return NodeInfo.create(this, Sinh::new, field());
    }

    @Override
    protected Sinh replaceChild(Expression newChild) {
        return new Sinh(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.SINH;
    }
}
