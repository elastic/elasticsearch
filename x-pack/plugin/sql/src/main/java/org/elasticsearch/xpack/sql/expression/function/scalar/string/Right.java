/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;

/**
 * Returns the rightmost count characters of a string.
 */
public class Right extends BinaryStringNumericFunction {

    public Right(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected BinaryStringNumericOperation operation() {
        return BinaryStringNumericOperation.RIGHT;
    }

    @Override
    protected Right replaceChildren(Expression newLeft, Expression newRight) {
        return new Right(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<Right> info() {
        return NodeInfo.create(this, Right::new, left(), right());
    }

}
