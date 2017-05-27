/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import static java.util.Collections.emptyList;

public abstract class Attribute extends NamedExpression {

    // empty - such as a top level attribute in SELECT cause
    // present - table name or a table name alias
    private final String qualifier;

    // can the attr be null - typically used in JOINs
    private final boolean nullable;

    public Attribute(Location location, String name, String qualifier, ExpressionId id) {
        this(location, name, qualifier, true, id);
    }

    public Attribute(Location location, String name, String qualifier, boolean nullable, ExpressionId id) {
        this(location, name, qualifier, nullable, id, false);
    }
    
    public Attribute(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, emptyList(), id, synthetic);
        this.qualifier = qualifier;
        this.nullable = nullable;
    }

    public String qualifier() {
        return qualifier;
    }

    public String qualifiedName() {
        return qualifier == null ? name() : qualifier.concat(".").concat(name());
    }

    public boolean nullable() {
        return nullable;
    }

    @Override
    public AttributeSet references() {
        return new AttributeSet(this);
    }

    public Attribute withQualifier(String qualifier) {
        return Objects.equals(qualifier(), qualifier) ? this : clone(location(), name(), dataType(), qualifier, nullable(), id(), synthetic());
    }

    public Attribute withName(String name) {
        return Objects.equals(name(), name) ? this : clone(location(), name, dataType(), qualifier(), nullable(), id(), synthetic());
    }

    public Attribute withNullability(boolean nullable) {
        return Objects.equals(nullable(), nullable) ? this : clone(location(), name(), dataType(), qualifier(), nullable, id(), synthetic());
    }
    
    public Attribute withId(ExpressionId id) {
        return Objects.equals(id(), id) ? this : clone(location(), name(), dataType(), qualifier(), nullable(), id, synthetic());
    }

    protected abstract Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic);

    @Override
    public Attribute toAttribute() {
        return this;
    }

    @Override
    public String toString() {
        return name() + "{" + label() + "}" + "#" + id();
    }

    protected abstract String label();
}