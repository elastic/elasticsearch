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

/**
 * <a href="https://en.wikipedia.org/wiki/Modulo_operation">Modulo</a>
 * function ({@code a % b}).
 * 
 * Note this operator is also registered as a function (needed for ODBC/SQL) purposes.
 */
public class Mod extends ArithmeticOperation {

    public Mod(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryArithmeticOperation.MOD);
    }

    @Override
    protected NodeInfo<Mod> info() {
        return NodeInfo.create(this, Mod::new, left(), right());
    }

    @Override
    protected Mod replaceChildren(Expression newLeft, Expression newRight) {
        return new Mod(source(), newLeft, newRight);
    }
}
