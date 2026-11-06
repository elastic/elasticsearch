/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * The closed vocabulary of cost model terms that a decision's attributed cost is broken into, so tests and UIs can rely on them.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum CostTerm {
    EXCHANGE_GATHER,
    EXCHANGE_HASH,
    EXCHANGE_BROADCAST,
    HASH_BUILD,
    HASH_PROBE,
    LOOKUP,
    MATERIALIZE,
    REPLAN,
    LOCAL_JOIN,
    PARTIAL_AGGREGATE,
    FINAL_AGGREGATE,
    STAGE_STARTUP;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
