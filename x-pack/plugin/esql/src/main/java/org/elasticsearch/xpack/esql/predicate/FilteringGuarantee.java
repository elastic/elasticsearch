/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * Whether a source may drop rows for a predicate. This is not a promise about false versus null.
 */
public enum FilteringGuarantee {
    /** The source cannot drop rows. */
    NONE,
    /** Dropped rows cannot satisfy the predicate. Retained rows may still fail and must be verified. */
    SUPERSET,
    /**
     * The produced rows are exactly the rows for which the predicate is true. A row that is not produced is not true.
     * It may be false or null; this guarantee does not say which.
     */
    EXACT_TRUE
}
