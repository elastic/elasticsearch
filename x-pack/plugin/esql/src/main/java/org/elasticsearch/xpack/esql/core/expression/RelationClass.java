/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * The kind of ES relation a row came from, as answered by {@link MetadataAttribute#RELATION_CLASS}.
 * <p>
 * Closed by construction: a relation kind maps to exactly one constant here, and there is no way to
 * spell a fourth value or misspell one of these three. A relation picks its kind by implementing the
 * matching {@code ClassifiedAs} nested interface rather than answering with a string literal of its
 * own, so the value a user sees cannot drift between relation kinds.
 * <p>
 * {@link #VIEW} has no relation to attach to: view resolution expands a view into the relations it
 * reads, so by execution there is no view left to ask. Nothing produces it today either: a query that
 * names a view and references {@code _class} fails with {@code Unknown column}, unless the view
 * resolves to a branch of its own alongside another source, where the column binds and the view's
 * rows answer NULL -- as they already do for {@code _index}. The value is declared because it is part
 * of the column's contract, not because anything answers it.
 * <p>
 * The wire value is lower-case and is part of the query contract: it appears in query results and in
 * {@code WHERE _class == "..."} predicates, so it must not change once released.
 */
public enum RelationClass {

    INDEX("index"),
    VIEW("view"),
    DATASET("dataset");

    private final String value;

    RelationClass(String value) {
        this.value = value;
    }

    /** The value surfaced to the user in the {@code _class} column. */
    public String value() {
        return value;
    }
}
