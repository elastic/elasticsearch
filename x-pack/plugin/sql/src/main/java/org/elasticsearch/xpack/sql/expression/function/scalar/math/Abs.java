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
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * <a href="https://en.wikipedia.org/wiki/Absolute_value">Absolute value</a>
 * function.
 */
public class Abs extends MathFunction {
    public Abs(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Abs> info() {
        return NodeInfo.create(this, Abs::new, field());
    }

    @Override
    protected Abs replaceChild(Expression newChild) {
        return new Abs(location(), newChild);
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
