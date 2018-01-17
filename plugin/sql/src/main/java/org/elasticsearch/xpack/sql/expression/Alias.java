/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import static java.util.Collections.singletonList;

import java.util.List;

public class Alias extends NamedExpression {

    private final Expression child;
    private final String qualifier;

    /**
     * Postpone attribute creation until it is actually created.
     * Being immutable, create only one instance.
     */
    private Attribute lazyAttribute;

    public Alias(Location location, String name, Expression child) {
        this(location, name, null, child, null);
    }

    public Alias(Location location, String name, String qualifier, Expression child) {
        this(location, name, qualifier, child, null);
    }

    public Alias(Location location, String name, String qualifier, Expression child, ExpressionId id) {
        this(location, name, qualifier, child, id, false);
    }

    public Alias(Location location, String name, String qualifier, Expression child, ExpressionId id, boolean synthetic) {
        super(location, name, singletonList(child), id, synthetic);
        this.child = child;
        this.qualifier = qualifier;
    }

    @Override
    protected NodeInfo<Alias> info() {
        return NodeInfo.create(this, Alias::new, name(), qualifier, child, id(), synthetic());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Alias(location(), name(), qualifier, newChildren.get(0), id(), synthetic());
    }

    public Expression child() {
        return child;
    }

    public String qualifier() {
        return qualifier;
    }

    @Override
    public boolean nullable() {
        return child.nullable();
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Attribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = createAttribute();
        }
        return lazyAttribute;
    }

    private Attribute createAttribute() {
        if (resolved()) {
            Expression c = child();

            Attribute attr = Expressions.attribute(c);
            if (attr != null) {
                return attr.clone(location(), name(), child.dataType(), qualifier, child.nullable(), id(), synthetic());
            }
            else {
                return new FieldAttribute(location(), null, name(), child.dataType(), qualifier, child.nullable(), id(), synthetic());
            }
        }

        return new UnresolvedAttribute(location(), name(), qualifier);
    }

    @Override
    public String toString() {
        return child + " AS " + name() + "#" + id();
    }
}
