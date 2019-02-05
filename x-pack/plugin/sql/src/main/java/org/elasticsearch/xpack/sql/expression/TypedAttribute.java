/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public abstract class TypedAttribute extends Attribute {

    private final DataType dataType;

    protected TypedAttribute(Source source, String name, DataType dataType, String qualifier, Nullability nullability,
            ExpressionId id, boolean synthetic) {
        super(source, name, qualifier, nullability, id, synthetic);
        this.dataType = dataType;
    }

    @Override
    public DataType dataType() {
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
