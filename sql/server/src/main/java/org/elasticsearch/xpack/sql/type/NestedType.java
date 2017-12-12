/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.util.Map;
import java.util.Objects;

public class NestedType implements CompoundDataType {

    private final Map<String, DataType> properties;

    public NestedType(Map<String, DataType> properties) {
        this.properties = properties;
    }

    public Map<String, DataType> properties() {
        return properties;
    }

    @Override
    public String esName() {
        return "nested";
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NestedType other = (NestedType) obj;
        return Objects.equals(properties, other.properties);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + esName() + "|" + sqlName() + "]=" + properties;
    }
}