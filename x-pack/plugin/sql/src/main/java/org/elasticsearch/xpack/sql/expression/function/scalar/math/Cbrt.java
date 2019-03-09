/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * <a href="https://en.wikipedia.org/wiki/Cube_root">Cube root</a>
 * function.
 */
public class Cbrt extends MathFunction {
    public Cbrt(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Cbrt> info() {
        return NodeInfo.create(this, Cbrt::new, field());
    }

    @Override
    protected Cbrt replaceChild(Expression newChild) {
        return new Cbrt(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.CBRT;
    }
}
