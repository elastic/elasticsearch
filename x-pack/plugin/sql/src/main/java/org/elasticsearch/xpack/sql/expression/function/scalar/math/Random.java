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
 * Returns a random double (using the given seed).
 */
public class Random extends MathFunction {

    public Random(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Random> info() {
        return NodeInfo.create(this, Random::new, field());
    }

    @Override
    protected Random replaceChild(Expression newChild) {
        return new Random(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.RANDOM;
    }
}
