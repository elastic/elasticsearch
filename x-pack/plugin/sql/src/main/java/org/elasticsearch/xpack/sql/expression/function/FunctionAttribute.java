/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.TypedAttribute;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public abstract class FunctionAttribute extends TypedAttribute {

    private final String functionId;

    protected FunctionAttribute(Source source, String name, DataType dataType, String qualifier, Nullability nullability,
                                ExpressionId id, boolean synthetic, String functionId) {
        super(source, name, dataType, qualifier, nullability, id, synthetic);
        this.functionId = functionId;
    }

    public String functionId() {
        return functionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), functionId);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(functionId, ((FunctionAttribute) obj).functionId());
    }
}
