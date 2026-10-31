/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * Whether an optimizer run searched and published decisions, so an empty run can be told apart from a failed one.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum RunStatus {
    /** The search completed and decisions were published. */
    OK,
    /** The optimizer is enabled but the run did not search. */
    INELIGIBLE,
    /** The search exceeded its budget, no decisions were published, and rule-based planning applies. */
    ABORTED;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
