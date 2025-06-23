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
        super(source, name, nullability, id, synthetic);
        this.dataType = dataType;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (TypedAttribute) o;
        return super.innerEquals(other) && dataType == other.dataType;
    }
}
