/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

/**
 * Represent a strongly typed parameter value
 */
public class QueryParam {

    private final String name;
    private final Object value;
    private final DataType type;
    private ContentLocation tokenLocation; // location of the token failing the parsing rules

    public QueryParam(String name, Object value, DataType type) {
        this.name = name;
        this.value = value;
        this.type = type;
    }

    public ContentLocation tokenLocation() {
        return tokenLocation;
    }

    public void tokenLocation(ContentLocation tokenLocation) {
        this.tokenLocation = tokenLocation;
    }

    public String name() {
        return this.name;
    }

    public Object value() {
        return this.value;
    }

    public String nameValue() {
        return "{" + this.name + ":" + this.value + "}";
    }

    public DataType type() {
        return this.type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryParam that = (QueryParam) o;
        return Objects.equals(value, that.value) && Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return value + " [" + name + "][" + type + "]";
    }
}
