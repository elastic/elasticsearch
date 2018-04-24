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
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

/**
 * <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_up">Round</a>
 * function.
 *
 * Note that this uses {@link Math#round(double)} which uses "half up" rounding
 * for `ROUND(-1.5)` rounds to `-1`.
 */
public class Round extends MathFunction {
    public Round(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Round> info() {
        return NodeInfo.create(this, Round::new, field());
    }

    @Override
    protected Round replaceChild(Expression newChild) {
        return new Round(location(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.ROUND;
    }

    @Override
    public DataType dataType() {
        return DataTypeConversion.asInteger(field().dataType());
    }
}
