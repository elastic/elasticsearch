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
 * The wire value is lower-case and is part of the query contract: it appears in query results and in
 * {@code WHERE _class == "..."} predicates, so it must not change once released.
 */
public enum RelationClass {

    INDEX("index"),
    VIEW("view"),
    DATASET("dataset"),
    SUBQUERY("subquery");

    private final String value;

    RelationClass(String value) {
        this.value = value;
    }

    /** The value surfaced to the user in the {@code _class} column. */
    public String value() {
        return value;
    }
}
