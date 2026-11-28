/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A source-neutral predicate. Adapters accept or decline these; none of them is a Lucene query or a Parquet filter.
 * This stands in for an ES|QL expression until the planner binds one.
 */
public sealed interface PredicateExpr {
    /** A field compared for equality. A null field value is null, not a match and not a miss. */
    record FieldEquals(String field, String value) implements PredicateExpr {}

    /** {@code left * right > threshold}. A null factor makes the comparison null. */
    record ProductGreaterThan(String left, String right, long threshold) implements PredicateExpr {}

    record And(PredicateExpr left, PredicateExpr right) implements PredicateExpr {}

    record Or(PredicateExpr left, PredicateExpr right) implements PredicateExpr {}

    record Not(PredicateExpr inner) implements PredicateExpr {}
}
