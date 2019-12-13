/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

/**
 * Converts an int ASCII code to a character value.
 */
public class Char extends UnaryStringIntFunction {

    public Char(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Char> info() {
        return NodeInfo.create(this, Char::new, field());
    }

    @Override
    protected Char replaceChild(Expression newChild) {
        return new Char(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.CHAR;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}