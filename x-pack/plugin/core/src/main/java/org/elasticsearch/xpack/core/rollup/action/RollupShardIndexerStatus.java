/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

/**
 * Status of the rollup indexer task
 */
public enum RollupShardIndexerStatus {
    INITIALIZED,
    STARTED,
    FAILED,
    COMPLETED,
    CANCELLED
}
