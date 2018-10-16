/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

public class LiteralAttribute extends TypedAttribute {

    private final Literal literal;

    public LiteralAttribute(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic,
            DataType dataType, Literal literal) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
        this.literal = literal;
    }

    @Override
    protected NodeInfo<LiteralAttribute> info() {
        return NodeInfo.create(this, LiteralAttribute::new,
            name(), qualifier(), nullable(), id(), synthetic(), dataType(), literal);
    }

    @Override
    protected LiteralAttribute clone(Location location, String name, String qualifier, boolean nullable,
                                     ExpressionId id, boolean synthetic) {
        return new LiteralAttribute(location, name, qualifier, nullable, id, synthetic, dataType(), literal);
    }

    @Override
    protected String label() {
        return "c";
    }

    @Override
    public Pipe asPipe() {
        return literal.asPipe();
    }
}