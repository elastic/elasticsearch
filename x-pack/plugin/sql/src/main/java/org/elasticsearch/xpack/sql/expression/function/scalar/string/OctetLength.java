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
 * Returns the number of bytes contained within the value expression.
 */
public class OctetLength extends UnaryStringFunction {

    public OctetLength(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<OctetLength> info() {
        return NodeInfo.create(this, OctetLength::new, field());
    }

    @Override
    protected OctetLength replaceChild(Expression newChild) {
        return new OctetLength(source(), newChild);
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.OCTET_LENGTH;
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }
}