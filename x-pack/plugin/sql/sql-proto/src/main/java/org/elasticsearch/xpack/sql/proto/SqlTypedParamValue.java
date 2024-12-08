/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.xpack.sql.proto.content.ContentLocation;

import java.util.Objects;

/**
 * Represent a strongly typed parameter value
 */
public class SqlTypedParamValue {

    public final Object value;
    public final String type;
    private boolean hasExplicitType;        // the type is explicitly set in the request or inferred by the parser
    private ContentLocation tokenLocation; // location of the token failing the parsing rules

    public SqlTypedParamValue(String type, Object value) {
        this(type, value, true);
    }

    public SqlTypedParamValue(String type, Object value, boolean hasExplicitType) {
        this.value = value;
        this.type = type;
        this.hasExplicitType = hasExplicitType;
    }

    public boolean hasExplicitType() {
        return hasExplicitType;
    }

    public void hasExplicitType(boolean hasExplicitType) {
        this.hasExplicitType = hasExplicitType;
    }

    public ContentLocation tokenLocation() {
        return tokenLocation;
    }

    public void tokenLocation(ContentLocation tokenLocation) {
        this.tokenLocation = tokenLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlTypedParamValue that = (SqlTypedParamValue) o;
        return Objects.equals(value, that.value)
            && Objects.equals(type, that.type)
            && Objects.equals(hasExplicitType, that.hasExplicitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, hasExplicitType);
    }

    @Override
    public String toString() {
        return String.valueOf(value) + " [" + type + "][" + hasExplicitType + "][" + tokenLocation + "]";
    }
}
