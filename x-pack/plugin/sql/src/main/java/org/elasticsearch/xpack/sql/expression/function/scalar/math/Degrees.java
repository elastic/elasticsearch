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
 * Convert from <a href="https://en.wikipedia.org/wiki/Radian">radians</a>
 * to <a href="https://en.wikipedia.org/wiki/Degree_(angle)">degrees</a>.
 */
public class Degrees extends MathFunction {
    public Degrees(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Degrees> info() {
        return NodeInfo.create(this, Degrees::new, field());
    }

    @Override
    protected Degrees replaceChild(Expression newChild) {
        return new Degrees(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.DEGREES;
    }
}
