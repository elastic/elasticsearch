/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

/**
 * Represents the optimization state for block processing in lookup/enrich operations.
 */
public enum BlockOptimization {
    /**
     * No optimization needed - use input page as-is.
     */
    NONE,
    /**
     * Dictionary optimization - input block has ordinals, can use dictionary block and ordinals.
     */
    DICTIONARY,
    /**
     * Range optimization - need to create range block for selected positions.
     */
    RANGE
}
