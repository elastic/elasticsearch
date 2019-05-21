/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Multiplication function ({@code a * b}).
 */
public class Mul extends ArithmeticOperation {

    private DataType dataType;

    public Mul(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryArithmeticOperation.MUL);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        DataType l = left().dataType();
        DataType r = right().dataType();

        // 1. both are numbers
        if (l.isNumeric() && r.isNumeric()) {
            return TypeResolution.TYPE_RESOLVED;
        }

        if (DataTypes.isInterval(l) && r.isInteger()) {
            dataType = l;
            return TypeResolution.TYPE_RESOLVED;
        } else if (DataTypes.isInterval(r) && l.isInteger()) {
            dataType = r;
            return TypeResolution.TYPE_RESOLVED;
        }

        return new TypeResolution(format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), l, r));
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = super.dataType();
        }
        return dataType;
    }

    @Override
    protected NodeInfo<Mul> info() {
        return NodeInfo.create(this, Mul::new, left(), right());
    }

    @Override
    protected Mul replaceChildren(Expression newLeft, Expression newRight) {
        return new Mul(source(), newLeft, newRight);
    }
}
