/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

/**
 * Marker for optional attributes. Acting as a dummy placeholder to avoid using null
 * in the tree (which is not allowed).
 */
public class EmptyAttribute extends Attribute {

    public EmptyAttribute(Source source) {
        super(source, StringUtils.EMPTY, null, null);
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
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
        return DataTypes.NULL;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }

    @Override
    public int hashCode() {
        return EmptyAttribute.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return true;
    }
}
