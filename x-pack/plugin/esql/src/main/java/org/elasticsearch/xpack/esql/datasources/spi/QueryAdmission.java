/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Shared wait timeout for per-query storage GET admission. Parquet is {@code compileOnly} on
 * esql, so this lives in the public SPI instead of beside the package-private budget.
 * <p>
 * One constant, not a cluster {@code Setting}: the query-budget acquire, the node limiter, and
 * (in a follow-up) the byte-watermark wait each use this same clock independently.
 */
public final class QueryAdmission {

    /** Default wait for a query-budget permit, in milliseconds. */
    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 60_000L;

    private QueryAdmission() {}
}
