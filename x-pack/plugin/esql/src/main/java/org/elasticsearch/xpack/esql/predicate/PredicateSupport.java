/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/** How completely a source can stand behind a predicate. */
public enum PredicateSupport {
    /** Retained rows are exactly the matches. Eliminated rows cannot match. */
    EXACT,
    /** Eliminated rows cannot match. Retained rows may still fail and must be verified. */
    SUPERSET,
    /** The source cannot apply this predicate. */
    UNSUPPORTED
}
