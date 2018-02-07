/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * <a href="https://en.wikipedia.org/wiki/Trigonometric_functions#cosine">Cosine</a>
 * function.
 */
public class Cos extends MathFunction {
    public Cos(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Cos> info() {
        return NodeInfo.create(this, Cos::new, field());
    }

    @Override
    protected Cos replaceChild(Expression newChild) {
        return new Cos(location(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.COS;
    }
}
