/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import java.util.Objects;

/**
 * Represent a strongly typed parameter value
 */
public class Param {

    public final String name;
    public final String type;
    public final Object value;
    private boolean hasExplicitType;        // the type is explicitly set in the request or inferred by the parser
    private ContentLocation tokenLocation; // location of the token failing the parsing rules

    public Param(String name, String type, Object value) {
        this(name, type, value, true);
    }

    public Param(String name, String type, Object value, boolean hasExplicitType) {
        this.name = name;
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
        Param that = (Param) o;
        return Objects.equals(value, that.value)
            && Objects.equals(type, that.type)
            && Objects.equals(hasExplicitType, that.hasExplicitType)
            && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, type, hasExplicitType);
    }

    @Override
    public String toString() {
        return value + " [" + name + "][" + type + "][" + hasExplicitType + "]";
    }
}
