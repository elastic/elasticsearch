/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * In a SQL statement, an Expression is whatever a user specifies inside an
 * action, so for instance:
 *
 * {@code SELECT a, b, ABS(c) FROM i}
 *
 * a, b, ABS(c), and i are all Expressions, with ABS(c) being a Function
 * (which is a type of expression) with a single child, c.
 */
public abstract class Expression extends Node<Expression> implements Resolvable {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        for (NamedWriteableRegistry.Entry e : NamedExpression.getNamedWriteables()) {
            entries.add(new NamedWriteableRegistry.Entry(Expression.class, e.name, in -> (NamedExpression) e.reader.read(in)));
        }
        entries.add(Literal.ENTRY);
        return entries;
    }

    public static class TypeResolution {
        private final boolean failed;
        private final String message;

        public static final TypeResolution TYPE_RESOLVED = new TypeResolution(false, StringUtils.EMPTY);

        public TypeResolution(String message) {
            this(true, message);
        }

        private TypeResolution(boolean unresolved, String message) {
            this.failed = unresolved;
            this.message = message;
        }

        public boolean unresolved() {
            return failed;
        }

        public boolean resolved() {
            return failed == false;
        }

        public TypeResolution and(TypeResolution other) {
            return failed ? this : other;
        }

        public TypeResolution and(Supplier<TypeResolution> other) {
            return failed ? this : other.get();
        }

        public String message() {
            return message;
        }

        @Override
        public String toString() {
            return resolved() ? "" : message;
        }
    }

    private TypeResolution lazyTypeResolution = null;
    private Boolean lazyChildrenResolved = null;
    private Expression lazyCanonical = null;
    private AttributeSet lazyReferences = null;

    public Expression(Source source, List<Expression> children) {
        super(source, children);
    }

    // whether the expression can be evaluated statically (folded) or not
    public boolean foldable() {
        return false;
    }

    public Object fold() {
        throw new QlIllegalArgumentException("Should not fold expression");
    }

    public abstract Nullability nullable();

    // the references/inputs/leaves of the expression tree
    public AttributeSet references() {
        if (lazyReferences == null) {
            lazyReferences = Expressions.references(children());
        }
        return lazyReferences;
    }

    public boolean childrenResolved() {
        if (lazyChildrenResolved == null) {
            lazyChildrenResolved = Boolean.valueOf(Resolvables.resolved(children()));
        }
        return lazyChildrenResolved;
    }

    /**
     * Does the tree rooted at this expression have valid types at all nodes?
     * <p>
     *     For example, {@code SIN(1.2)} has a valid type and should return
     *     {@link TypeResolution#TYPE_RESOLVED} to signal "this type is fine".
     *     Another example, {@code SIN("cat")} has an invalid type in the
     *     tree. The value passed to the {@code SIN} function is a string which
     *     doesn't make any sense. So this method should return a "failure"
     *     resolution which it can build by calling {@link TypeResolution#TypeResolution(String)}.
     * </p>
     * <p>
     *     Take {@code SIN(1.2) + COS(ATAN("cat"))}, this tree should also
     *     fail, specifically because {@code ATAN("cat")} is invalid. This should
     *     fail even though {@code +} is perfectly valid when run on the results
     *     of {@code SIN} and {@code COS}. And {@code COS} can operate on the results
     *     of any valid call to {@code ATAN}. For this method to return a "valid"
     *     result the <strong>whole</strong> tree rooted at this expression must
     *     be valid.
     * </p>
     */
    public final TypeResolution typeResolved() {
        if (lazyTypeResolution == null) {
            lazyTypeResolution = resolveType();
        }
        return lazyTypeResolution;
    }

    /**
     * The implementation of {@link #typeResolved}, which is just a caching wrapper
     * around this method. See it's javadoc for what this method should return.
     * <p>
     *     Implementations will rarely interact with the {@link TypeResolution}
     *     class directly, instead usually calling the utility methods on {@link TypeResolutions}.
     * </p>
     * <p>
     *     Implementations should fail if {@link #childrenResolved()} returns {@code false}.
     * </p>
     */
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
        if (children().isEmpty()) {
            return this;
        }
        List<Expression> canonicalChildren = Expressions.canonicalize(children());
        // check if replacement is really needed
        if (children().equals(canonicalChildren)) {
            return this;
        }
        return replaceChildrenSameSize(canonicalChildren);
    }

    public boolean semanticEquals(Expression other) {
        return canonical().equals(other.canonical());
    }

    public int semanticHash() {
        return canonical().hashCode();
    }

    @Override
    public boolean resolved() {
        return childrenResolved() && typeResolved().resolved();
    }

    /**
     * The {@link DataType} returned by executing the tree rooted at this
     * expression. If {@link #typeResolved()} returns an error then the behavior
     * of this method is undefined. It <strong>may</strong> return a valid
     * type. Or it may throw an exception. Or it may return a totally nonsensical
     * type.
     */
    public abstract DataType dataType();

    @Override
    public String toString() {
        return sourceText();
    }

    @Override
    public String propertiesToString(boolean skipIfChild) {
        return super.propertiesToString(false);
    }
}
