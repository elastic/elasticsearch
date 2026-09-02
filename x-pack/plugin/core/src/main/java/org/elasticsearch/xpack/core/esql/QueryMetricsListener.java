/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

import java.util.Map;

/**
 * Called once per completed ES|QL query on the coordinating node.
 * Implementations must be cheap and non-blocking — no I/O, no locks, only atomic operations.
 * <p>
 * Currently, the metrics are only collected when the query has federated sources. The metrics collected are raw
 * profile-based metrics, no computation is performed on them at collection time.
 * </p>
 */
public interface QueryMetricsListener {

    QueryMetricsListener NOOP = metrics -> {};

    /**
     * Time spent on planning, from queryProfile.planning()
     */
    String PLANNING_NANOS = "planning_nanos";
    /**
     * Time spent by drivers on compute, from result.completionInfo().cpuNanos()
     */
    String CPU_NANOS = "cpu_nanos";
    /**
     * Time spent by drivers on reading data, from result.completionInfo().readNanos()
     */
    String READ_NANOS = "read_nanos";
    /**
     * CPU time spent by drivers on reading data (no IO wait), from result.completionInfo().readCpuNanos()
     */
    String READ_CPU_NANOS = "read_cpu_nanos";
    /**
     * Time spent on discovering splits, from queryProfile.splitDiscoveryNanos()
     */
    String SPLIT_DISCOVERY_NANOS = "split_discovery_nanos";
    /**
     * Bytes read by the query, both from external sources and Lucene.
     */
    String BYTES_READ = "bytes_read";

    void onQueryCompleted(Map<String, Long> metrics);
}
