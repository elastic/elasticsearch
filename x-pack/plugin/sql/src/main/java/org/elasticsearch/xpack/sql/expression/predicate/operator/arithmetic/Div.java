/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

/**
 * Division function ({@code a / b}).
 */
public class Div extends SqlArithmeticOperation {

    public Div(Source source, Expression left, Expression right) {
        super(source, left, right, SqlBinaryArithmeticOperation.DIV);
    }

    @Override
    protected NodeInfo<Div> info() {
        return NodeInfo.create(this, Div::new, left(), right());
    }

    @Override
    protected Div replaceChildren(Expression newLeft, Expression newRight) {
        return new Div(source(), newLeft, newRight);
    }

    @Override
    public DataType dataType() {
        return SqlDataTypeConverter.commonType(left().dataType(), right().dataType());
    }
}
