/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.List;
import java.util.Locale;

import org.elasticsearch.xpack.sql.capabilities.Resolvable;
import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.tree.NodeUtils;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import static java.lang.String.format;

public abstract class Expression extends Node<Expression> implements Resolvable {

    public static class TypeResolution {
        private final boolean failed;
        private final String message;
        
        public static final TypeResolution TYPE_RESOLVED = new TypeResolution(false, StringUtils.EMPTY);

        public TypeResolution(String message, Object... args) {
            this(true, format(Locale.ROOT, message, args));
        }

        private TypeResolution(boolean unresolved, String message) {
            this.failed = unresolved;
            this.message = message;
        }

        public boolean unresolved() {
            return failed;
        }

        public boolean resolved() {
            return !failed;
        }

        public String message() {
            return message;
        }
    }

    private TypeResolution lazyTypeResolution = null;
    private Boolean lazyChildrenResolved = null;
    private Expression lazyCanonical = null;

    public Expression(Location location, List<Expression> children) {
        super(location, children);
    }

    // whether the expression can be evaluated statically (folded) or not
    public boolean foldable() {
        return false;
    }

    public Object fold() {
        return null;
    }

    public abstract boolean nullable();

    public AttributeSet references() {
        return Expressions.references(children());
    }

    public boolean childrenResolved() {
        if (lazyChildrenResolved == null) {
            lazyChildrenResolved = Boolean.valueOf(Resolvables.resolved(children()));
        }
        return lazyChildrenResolved;
    }

    public TypeResolution typeResolved() {
        if (lazyTypeResolution == null) {
            lazyTypeResolution = resolveType();
        }
        return lazyTypeResolution;
    }

    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    public final Expression canonical() {
        if (lazyCanonical == null) {
            lazyCanonical = canonicalize();
        }
        return lazyCanonical;
    }

    protected Expression canonicalize() {
        return this;
    }

    public final boolean canonicalEquals(Expression other) {
        return canonical().equals(other.canonical());
    }

    public final int canonicalHash() {
        return canonical().hashCode();
    }

    @Override
    public boolean resolved() {
        return childrenResolved() && typeResolved().resolved();
    }

    public abstract DataType dataType();

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
        return nodeName() + "[" + NodeUtils.propertiesToString(this, false) + "]";
    }
}