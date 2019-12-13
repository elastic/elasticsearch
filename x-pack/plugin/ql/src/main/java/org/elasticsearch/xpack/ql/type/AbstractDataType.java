/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import java.util.Objects;

public abstract class AbstractDataType implements DataTyp {

    private final String typeName;
    private final String esType;
    private final int size;

    public AbstractDataType(String esType, String typeName, int size) {
        this.esType = esType;
        this.typeName = typeName;
        this.size = size;
    }

    @Override
    public String esType() {
        return esType;
    }

    @Override
    public String typeName() {
        return typeName;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int hashCode() {
        return esType.hashCode();
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
        return Objects.equals(esType, other.esType);
    }
}
