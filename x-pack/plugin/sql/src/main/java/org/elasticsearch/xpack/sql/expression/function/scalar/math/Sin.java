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
 * <a href="https://en.wikipedia.org/wiki/Trigonometric_functions#sine">Sine</a>
 * fuction.
 */
public class Sin extends MathFunction {
    public Sin(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sin> info() {
        return NodeInfo.create(this, Sin::new, field());
    }

    @Override
    protected Sin replaceChild(Expression newChild) {
        return new Sin(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.SIN;
    }
}
