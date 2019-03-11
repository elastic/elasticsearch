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
 * <a href="https://en.wikipedia.org/wiki/Inverse_trigonometric_functions">Arc cosine</a>
 * function.
 */
public class ACos extends MathFunction {
    public ACos(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<ACos> info() {
        return NodeInfo.create(this, ACos::new, field());
    }

    @Override
    protected ACos replaceChild(Expression newChild) {
        return new ACos(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.ACOS;
    }
}
