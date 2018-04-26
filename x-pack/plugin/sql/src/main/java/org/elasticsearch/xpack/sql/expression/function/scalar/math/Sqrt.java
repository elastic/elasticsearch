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
 * <a href="https://en.wikipedia.org/wiki/Square_root">Square root</a>
 * function.
 */
public class Sqrt extends MathFunction {
    public Sqrt(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Sqrt> info() {
        return NodeInfo.create(this, Sqrt::new, field());
    }

    @Override
    protected Sqrt replaceChild(Expression newChild) {
        return new Sqrt(location(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.SQRT;
    }
}
