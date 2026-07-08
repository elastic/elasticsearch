/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import java.util.Map;

/**
 * Called once per completed ES|QL query (sync, async, PromQL) on the coordinating node.
 * Implementations must be cheap and non-blocking — no I/O, no locks, only atomic operations.
 *
 * <p>Loaded via {@link org.elasticsearch.plugins.ExtensiblePlugin} so external modules
 * (e.g. the serverless metering module) can register a collector without coupling to
 * compute data classes. The metrics map keys are the stable string constants defined here.
 */
public interface EsqlQueryMetricsCollector {

    EsqlQueryMetricsCollector NOOP = metrics -> {};

    String PLANNING_NANOS = "planning_nanos";
    String CPU_NANOS = "cpu_nanos";
    String READ_NANOS = "read_nanos";
    String BYTES_READ = "bytes_read";

    /**
     * Called on the SEARCH thread pool once the coordinating node has the final aggregated result.
     * For async queries this fires on real compute completion, not on the initial response.
     */
    void onQueryCompleted(Map<String, Long> metrics);
}
