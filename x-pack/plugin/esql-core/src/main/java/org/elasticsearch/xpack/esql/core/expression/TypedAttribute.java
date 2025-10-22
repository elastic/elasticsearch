/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

/**
 * A fully resolved attribute - we know its type. For example, if it references data directly from Lucene, this will be a
 * {@link FieldAttribute}. If it references the results of another calculation it will be {@link ReferenceAttribute}s.
 */
public abstract class TypedAttribute extends Attribute {

    private final DataType dataType;

    protected TypedAttribute(
        Source source,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        this(source, null, name, dataType, nullability, id, synthetic);
    }

    protected TypedAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        super(source, qualifier, name, nullability, id, synthetic);
        this.dataType = dataType;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), dataType);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (TypedAttribute) o;
        return super.innerEquals(other, ignoreIds) && dataType == other.dataType;
    }
}
