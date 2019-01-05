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
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Returns the sign of the given expression:
 * <ul>
 * <li>-1 if it is negative</li>
 * <li> 0 if it is zero</li>
 * <li>+1 if it is positive</li>
 * </ul>
 */
public class Sign extends MathFunction {
    public Sign(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sign> info() {
        return NodeInfo.create(this, Sign::new, field());
    }

    @Override
    protected Sign replaceChild(Expression newChild) {
        return new Sign(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.SIGN;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}
