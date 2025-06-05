/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.io.IOException;

/**
 * Marker for optional attributes. Acting as a dummy placeholder to avoid using null
 * in the tree (which is not allowed).
 */
public class EmptyAttribute extends Attribute {
    public EmptyAttribute(Source source) {
        super(source, StringUtils.EMPTY, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    protected Attribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        return this;
    }

    @Override
    protected String label() {
        return "e";
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public DataType dataType() {
        return DataType.NULL;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return EmptyAttribute.class.hashCode();
    }

    @Override
    protected boolean innerEquals(Object o) {
        return true;
    }
}
