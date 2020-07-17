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
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

/**
 * Lowercases all uppercase letters in a string.
 */
public class LCase extends UnaryStringFunction {

    public LCase(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<LCase> info() {
        return NodeInfo.create(this, LCase::new, field());
    }

    @Override
    protected LCase replaceChild(Expression newChild) {
        return new LCase(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.LCASE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }
}
