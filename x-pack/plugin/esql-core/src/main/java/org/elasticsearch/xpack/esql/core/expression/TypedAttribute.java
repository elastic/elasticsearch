/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataTypes;

import java.util.Objects;

public abstract class TypedAttribute extends Attribute {

    private final DataTypes dataType;

    protected TypedAttribute(
        Source source,
        String name,
        DataTypes dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        super(source, name, qualifier, nullability, id, synthetic);
        this.dataType = dataType;
    }

    @Override
    public DataTypes dataType() {
        return dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(dataType, ((TypedAttribute) obj).dataType);
    }
}
