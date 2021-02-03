/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.Objects;

public class ArrayDataType extends DataType {
    /**
     * The base type of an array type.
     */
    private final DataType baseType;

    private ArrayDataType(DataType baseType) {
        super(baseType.name() + "_ARRAY", baseType.esType(), baseType.size(), baseType.isInteger(), baseType.isRational(),
            baseType.hasDocValues());
        this.baseType = baseType;
    }

    public static ArrayDataType of(DataType baseType) {
        // no multidimensional arrays supported
        if (baseType instanceof ArrayDataType) {
            throw new QlIllegalArgumentException("the base type of an array type cannot be itself an array type; provided: " + baseType);
        }
        return new ArrayDataType(baseType);
    }

    public DataType baseType() {
        return baseType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(baseType, ((ArrayDataType) obj).baseType());
    }
}
