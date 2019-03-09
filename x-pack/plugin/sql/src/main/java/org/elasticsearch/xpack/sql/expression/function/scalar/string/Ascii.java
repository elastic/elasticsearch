/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Returns the ASCII code of the leftmost character of the given (char) expression.
 */
public class Ascii extends UnaryStringFunction {

    public Ascii(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Ascii> info() {
        return NodeInfo.create(this, Ascii::new, field());
    }

    @Override
    protected Ascii replaceChild(Expression newChild) {
        return new Ascii(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.ASCII;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}
