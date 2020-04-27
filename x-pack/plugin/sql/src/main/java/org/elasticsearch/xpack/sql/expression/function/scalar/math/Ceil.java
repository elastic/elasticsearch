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
 * <a href="https://en.wikipedia.org/wiki/Floor_and_ceiling_functions">Ceiling</a>
 * function.
 */
public class Ceil extends MathFunction {
    public Ceil(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Ceil> info() {
        return NodeInfo.create(this, Ceil::new, field());
    }

    @Override
    protected Ceil replaceChild(Expression newChild) {
        return new Ceil(source(), newChild);
    }

    @Override
    public Number fold() {
        Object result = super.fold();
        if (result == null) {
            return null;
        }
        return DataTypeConverter.toInteger((double) result, dataType());
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.CEIL;
    }

    @Override
    public DataType dataType() {
        return DataTypeConverter.asInteger(field().dataType());
    }
}
