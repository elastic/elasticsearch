/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import java.util.Map;

/**
 * Called once per completed ES|QL query on the coordinating node.
 * Implementations must be cheap and non-blocking — no I/O, no locks, only atomic operations.
 */
public interface EsqlQueryMetricsCollector {

    EsqlQueryMetricsCollector NOOP = metrics -> {};

    String PLANNING_NANOS = "planning_nanos";
    String CPU_NANOS = "cpu_nanos";
    String READ_NANOS = "read_nanos";
    String BYTES_READ = "bytes_read";

    /**
     * Called on the coordinating node once it has the final aggregated result.
     * For async queries this is called on real compute completion.
     */
    void onQueryCompleted(Map<String, Long> metrics);
}
