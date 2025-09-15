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
 * A resolved attribute - we know its type.
 * Because the name alone is not sufficient to identify an attribute (two different relations can have the same attribute name),
 * we also have an id that is used in equality checks and hashing.
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
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), id(), dataType);
    }

    /**
     * After resolution, the name id uniquely identifies an attribute, so it is included in equality check.
     */
    @Override
    protected boolean innerEquals(Object o) {
        var other = (TypedAttribute) o;
        return super.innerEquals(other) && Objects.equals(id(), other.id()) && dataType == other.dataType;
    }
}
