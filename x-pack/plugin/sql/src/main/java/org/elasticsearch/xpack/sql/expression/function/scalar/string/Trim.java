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
 * Trims both leading and trailing whitespaces.
 */
public class Trim extends UnaryStringFunction {

    public Trim(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Trim> info() {
        return NodeInfo.create(this, Trim::new, field());
    }

    @Override
    protected Trim replaceChild(Expression newChild) {
        return new Trim(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.TRIM;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }
}
