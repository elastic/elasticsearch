/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Objects;

/**
 * An {@link Attribute} that contains a {@link Block} of values.
 */
public class TableColumnAttribute extends TypedAttribute {
    private final Block block;

    public TableColumnAttribute(Source source, String name, DataType dataType, NameId id, Block block) {
        super(source, name, dataType, null, Nullability.UNKNOWN, id, false);
        this.block = block;
    }

    public Block block() {
        return block;
    }

    @Override
    protected TableColumnAttribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new TableColumnAttribute(source, name, type, id, block);
    }

    @Override
    protected String label() {
        return "tc";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TableColumnAttribute::new, name(), dataType(), id(), block);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && block.equals(((TableColumnAttribute) o).block);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), block);
    }
}
