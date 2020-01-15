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
 * <a href="https://en.wikipedia.org/wiki/Square_root">Square root</a>
 * function.
 */
public class Sqrt extends MathFunction {
    public Sqrt(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sqrt> info() {
        return NodeInfo.create(this, Sqrt::new, field());
    }

    @Override
    protected Sqrt replaceChild(Expression newChild) {
        return new Sqrt(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.SQRT;
    }
}
