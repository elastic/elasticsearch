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
 * <a href="https://en.wikipedia.org/wiki/Trigonometric_functions#Cosecant,_secant,_and_cotangent">Cotangent</a>
 * function.
 */
public class Cot extends MathFunction {
    public Cot(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Cot> info() {
        return NodeInfo.create(this, Cot::new, field());
    }

    @Override
    protected Cot replaceChild(Expression newChild) {
        return new Cot(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.COT;
    }
}
