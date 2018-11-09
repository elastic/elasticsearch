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
 * Generates a string consisting of count spaces.
 */
public class Space extends UnaryStringIntFunction {

    public Space(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Space> info() {
        return NodeInfo.create(this, Space::new, field());
    }

    @Override
    protected Space replaceChild(Expression newChild) {
        return new Space(location(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.SPACE;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}