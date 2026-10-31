/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * Which planning step started an optimizer run, since one query can be optimized again after each executed subplan.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum Trigger {
    /** The first optimization of the query's plan. */
    INITIAL,
    /** A re-optimization of the main plan after a subplan produced its result. */
    AFTER_SUBPLAN;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
