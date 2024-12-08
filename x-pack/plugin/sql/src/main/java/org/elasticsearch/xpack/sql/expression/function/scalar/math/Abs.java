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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

/**
 * <a href="https://en.wikipedia.org/wiki/Absolute_value">Absolute value</a>
 * function.
 */
public class Abs extends MathFunction {
    public Abs(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Abs> info() {
        return NodeInfo.create(this, Abs::new, field());
    }

    @Override
    protected Abs replaceChild(Expression newChild) {
        return new Abs(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.ABS;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }
}
