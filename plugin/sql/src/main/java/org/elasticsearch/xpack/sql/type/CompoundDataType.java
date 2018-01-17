/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Map;
import java.util.Objects;

public abstract class CompoundDataType extends AbstractDataType {

    private final Map<String, DataType> properties;

    CompoundDataType(JDBCType sqlType, boolean hasDocValues, Map<String, DataType> properties) {
        super(sqlType, hasDocValues);
        this.properties = properties;
    }

    public Map<String, DataType> properties() {
        return properties;
    }

    @Override
    public int precision() {
        return 0;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isRational() {
        return false;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Objects.hash(properties));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(properties, ((CompoundDataType) obj).properties);
    }
}
