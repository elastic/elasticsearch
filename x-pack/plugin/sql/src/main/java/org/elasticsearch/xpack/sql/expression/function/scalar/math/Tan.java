/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

/**
 * <a href="https://en.wikipedia.org/wiki/Trigonometric_functions#sine">Tangent</a>
 * function.
 */
public class Tan extends MathFunction {
    public Tan(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Tan> info() {
        return NodeInfo.create(this, Tan::new, field());
    }

    @Override
    protected Tan replaceChild(Expression newChild) {
        return new Tan(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.TAN;
    }
}
