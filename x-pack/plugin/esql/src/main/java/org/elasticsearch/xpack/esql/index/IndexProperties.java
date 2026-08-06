/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.index.IndexMode;

/**
 * Per-concrete-index properties collected at field-caps / index-resolution time.
 * <p>
 * {@link #numberOfShards} is the total primary shard count for the index.
 * A value of {@code 0} means the count was not available (e.g. the coordinating
 * node is on a transport version that pre-dates shard-count propagation, or the
 * index type does not report shard counts). Consumers must treat {@code 0} as
 * "unknown".
 * </p>
 */
public record IndexProperties(IndexMode indexMode, int numberOfShards) {
    /**
     * Convenience constructor that infers the shard count from the index mode.
     * {@link IndexMode#LOOKUP} indices always have exactly 1 shard; all other modes default to 0 (unknown).
     */
    public IndexProperties(IndexMode indexMode) {
        this(indexMode, indexMode == IndexMode.LOOKUP ? 1 : 0);
    }
}
