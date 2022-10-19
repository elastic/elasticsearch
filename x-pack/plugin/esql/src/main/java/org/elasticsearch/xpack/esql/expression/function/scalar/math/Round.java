/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

public class Round extends UnaryScalarFunction {

    public Round(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return null;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return null;
    }

    @Override
    protected Processor makeProcessor() {
        return null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }
}
