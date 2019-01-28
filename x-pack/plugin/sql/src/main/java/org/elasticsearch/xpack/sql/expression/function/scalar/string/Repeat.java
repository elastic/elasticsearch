/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * Creates a string composed of a string repeated count times.
 */
public class Repeat extends BinaryStringNumericFunction {

    public Repeat(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected BinaryStringNumericOperation operation() {
        return BinaryStringNumericOperation.REPEAT;
    }

    @Override
    protected Repeat replaceChildren(Expression newLeft, Expression newRight) {
        return new Repeat(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<Repeat> info() {
        return NodeInfo.create(this, Repeat::new, left(), right());
    }
}
