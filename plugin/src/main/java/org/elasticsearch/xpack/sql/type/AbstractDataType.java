/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Objects;

abstract class AbstractDataType implements DataType {

    private final JDBCType sqlType;
    private final boolean hasDocValues;

    AbstractDataType(JDBCType sqlType, boolean hasDocValues) {
        this.sqlType = sqlType;
        this.hasDocValues = hasDocValues;
    }

    @Override
    public boolean hasDocValues() {
        return hasDocValues;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public JDBCType sqlType() {
        return sqlType;
    }

    @Override
    public String toString() {
        return esName();
    }

    @Override
    public int hashCode() {
        return esName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        AbstractDataType other = (AbstractDataType) obj;
        return Objects.equals(esName(), other.esName());
    }
}
