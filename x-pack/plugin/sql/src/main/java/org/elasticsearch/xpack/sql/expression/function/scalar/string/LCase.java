/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Lowercases all uppercase letters in a string.
 */
public class LCase extends UnaryStringFunction {

    public LCase(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<LCase> info() {
        return NodeInfo.create(this, LCase::new, field());
    }

    @Override
    protected LCase replaceChild(Expression newChild) {
        return new LCase(location(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.LCASE;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}
