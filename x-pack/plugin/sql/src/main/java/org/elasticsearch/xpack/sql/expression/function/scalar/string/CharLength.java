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
 * Returns the length (in characters) of the string expression.
 */
public class CharLength extends UnaryStringFunction {

    public CharLength(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<CharLength> info() {
        return NodeInfo.create(this, CharLength::new, field());
    }

    @Override
    protected CharLength replaceChild(Expression newChild) {
        return new CharLength(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.CHAR_LENGTH;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}