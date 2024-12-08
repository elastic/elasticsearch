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
 * Convert from <a href="https://en.wikipedia.org/wiki/Degree_(angle)">degrees</a>
 * to <a href="https://en.wikipedia.org/wiki/Radian">radians</a>.
 */
public class Radians extends MathFunction {
    public Radians(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Radians> info() {
        return NodeInfo.create(this, Radians::new, field());
    }

    @Override
    protected Radians replaceChild(Expression newChild) {
        return new Radians(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.RADIANS;
    }
}
