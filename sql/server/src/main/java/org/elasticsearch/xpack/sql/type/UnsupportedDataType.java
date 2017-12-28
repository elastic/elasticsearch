/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.analysis.index.MappingException;

import java.sql.JDBCType;
import java.util.Objects;

public class UnsupportedDataType extends AbstractDataType {

    private final String esType;

    UnsupportedDataType(String esType) {
        super(JDBCType.OTHER, false);
        this.esType = esType;
    }

    @Override
    public String sqlName() {
        return "UNSUPPORTED";
    }

    @Override
    public Object defaultValue() {
        throw new MappingException("Unsupported Elasticsearch type " + esType);
    }

    @Override
    public boolean isInteger() {
        throw new MappingException("Unsupported Elasticsearch type " + esType);
    }

    @Override
    public boolean isRational() {
        throw new MappingException("Unsupported Elasticsearch type " + esType);
    }

    @Override
    public boolean same(DataType other) {
        return (other instanceof UnsupportedDataType) && Objects.equals(esType, ((UnsupportedDataType) other).esType);
    }

    @Override
    public String esName() {
        return esType;
    }

    @Override
    public boolean hasDocValues() {
        throw new MappingException("Unsupported Elasticsearch type " + esType);
    }

    @Override
    public boolean isPrimitive() {
        throw new MappingException("Unsupported Elasticsearch type " + esType);
    }
}