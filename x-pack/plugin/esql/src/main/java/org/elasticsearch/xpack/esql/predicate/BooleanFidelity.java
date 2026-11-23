/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/** Whether a source can assign true, false, and null to rows already in hand. */
public enum BooleanFidelity {
    /** No per-row Boolean result. */
    NONE,
    /** Identifies the true rows only. The others are not split into false and null. */
    TRUE_ONLY,
    /** Returns true, false, and null. Required for {@code EVAL} and for {@code NOT}. */
    THREE_VALUED
}
