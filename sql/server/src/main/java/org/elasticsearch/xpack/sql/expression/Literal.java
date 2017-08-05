/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.Objects;

public class Literal extends LeafExpression {

    private final Object value;
    private final DataType dataType;

    public static final Literal TRUE = Literal.of(Location.EMPTY, Boolean.TRUE);
    public static final Literal FALSE = Literal.of(Location.EMPTY, Boolean.FALSE);

    public Literal(Location location, Object value, DataType dataType) {
        super(location);
        this.value = value;
        this.dataType = dataType;
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public boolean nullable() {
        return value == null;
    }

    public DataType dataType() {
        return dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    @Override
    public boolean resolved() {
        return true;
    }

    public Object fold() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Literal other = (Literal) obj;
        return Objects.equals(value, other.value)
                && Objects.equals(dataType, other.dataType);
    }

    @Override
    public String toString() {
        return Objects.toString(value);
    }

    public static Literal of(Location loc, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(loc, value, DataTypes.fromJava(value));
    }
}