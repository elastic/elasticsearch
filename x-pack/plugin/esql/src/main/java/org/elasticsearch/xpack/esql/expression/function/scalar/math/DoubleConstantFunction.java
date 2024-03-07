/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.qlcore.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

/**
 * Function that emits constants, like Euler's number.
 */
public abstract class DoubleConstantFunction extends ScalarFunction {
    protected DoubleConstantFunction(Source source) {
        super(source);
    }

    @Override
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
