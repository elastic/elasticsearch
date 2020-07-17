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
 * <a href="https://en.wikipedia.org/wiki/Exponential_function">e<sup>x</sup></a>
 * function.
 */
public class Exp extends MathFunction {
    public Exp(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Exp> info() {
        return NodeInfo.create(this, Exp::new, field());
    }

    @Override
    protected Exp replaceChild(Expression newChild) {
        return new Exp(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.EXP;
    }
}
