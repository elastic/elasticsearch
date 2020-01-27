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
 * Uppercases all lowercase letters in a string.
 */
public class UCase extends UnaryStringFunction {

    public UCase(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<UCase> info() {
        return NodeInfo.create(this, UCase::new, field());
    }

    @Override
    protected UCase replaceChild(Expression newChild) {
        return new UCase(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.UCASE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

}
