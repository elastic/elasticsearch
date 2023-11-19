/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

/**
 * Function that emits a pseudo random number.
 */
public class Random extends ScalarFunction {

    public Random(Source source) {
        super(source);
    }

    @Override
    public Object fold() {
        return Math.random();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Random(source());
    }

    public final boolean foldable() {
        return true;
    }

    @Override
    public final DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public final ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    protected final NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }
}
