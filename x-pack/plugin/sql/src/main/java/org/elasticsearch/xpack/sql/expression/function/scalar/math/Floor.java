/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

/**
 * <a href="https://en.wikipedia.org/wiki/Floor_and_ceiling_functions">Floor</a>
 * function.
 */
public class Floor extends MathFunction {
    public Floor(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Floor> info() {
        return NodeInfo.create(this, Floor::new, field());
    }

    @Override
    protected Floor replaceChild(Expression newChild) {
        return new Floor(source(), newChild);
    }

    @Override
    public Object fold() {
        Object result = super.fold();
        if (result == null) {
            return null;
        }
        return DataTypeConverter.toInteger((double) result, dataType());
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.FLOOR;
    }

    @Override
    public DataType dataType() {
        return DataTypeConverter.asInteger(field().dataType());
    }
}
