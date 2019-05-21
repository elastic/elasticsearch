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
 * Returns the number of bits contained within the value expression.
 */
public class BitLength extends UnaryStringFunction {

    public BitLength(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<BitLength> info() {
        return NodeInfo.create(this, BitLength::new, field());
    }

    @Override
    protected BitLength replaceChild(Expression newChild) {
        return new BitLength(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.BIT_LENGTH;
    }

    @Override
    public DataType dataType() {
        //TODO investigate if a data type Long (BIGINT) wouldn't be more appropriate here
        return DataType.INTEGER;
    }
}