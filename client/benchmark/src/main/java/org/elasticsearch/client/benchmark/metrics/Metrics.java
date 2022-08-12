/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.metrics;

public record Metrics(
    String operation,
    long successCount,
    long errorCount,
    double throughput,
    double serviceTimeP50,
    double serviceTimeP90,
    double serviceTimeP95,
    double serviceTimeP99,
    double serviceTimeP999,
    double serviceTimeP9999,
    double latencyP50,
    double latencyP90,
    double latencyP95,
    double latencyP99,
    double latencyP999,
    double latencyP9999
) {}
