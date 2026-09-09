/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Function that emits constants, like Euler’s number.
 * <p>
 * These are nullary, so they satisfy {@link AnyNullIsNull} vacuously: there is no argument that could be null.
 */
public abstract class DoubleConstantFunction extends ScalarFunction implements AnyNullIsNull {
    protected DoubleConstantFunction(Source source) {
        super(source);
    }

    @Override
    public final boolean foldable() {
        return true;
    }

    @Override
    public final DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected final NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }
}
