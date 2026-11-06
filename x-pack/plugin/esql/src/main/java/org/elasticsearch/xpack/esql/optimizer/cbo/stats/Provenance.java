/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

/** Describes the evidence supporting a planning estimate. */
public enum Provenance {
    EXACT,
    METADATA,
    SAMPLED,
    DERIVED,
    DEFAULT,
    UNKNOWN
}
