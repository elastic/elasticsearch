/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Location;

public abstract class NamedExpression extends Expression {

    private final String name;
    private final ExpressionId id;
    private final boolean synthetic;

    public NamedExpression(Location location, String name, List<Expression> children, ExpressionId id) {
        this(location, name, children, id, false);
    }
    
    public NamedExpression(Location location, String name, List<Expression> children, ExpressionId id, boolean synthetic) {
        super(location, children);
        this.name = name;
        this.id = (id == null ? ExpressionIdGenerator.newId() : id);
        this.synthetic = synthetic;
    }

    public String name() {
        return name;
    }

    public ExpressionId id() {
        return id;
    }

    public boolean synthetic() {
        return synthetic;
    }

    public abstract Attribute toAttribute();

    @Override
    public final int hashCode() {
        // NOCOMMIT making this final upsets checkstyle.
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NamedExpression other = (NamedExpression) obj;
        return Objects.equals(synthetic, other.synthetic)
                && Objects.equals(id, other.id) 
                && Objects.equals(name(), other.name())
                && Objects.equals(children(), other.children());
    }
}
