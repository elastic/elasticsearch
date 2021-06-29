/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.metrics;

public final class Metrics {
    public final String operation;
    public final long successCount;
    public final long errorCount;
    public final double throughput;
    public final double serviceTimeP50;
    public final double serviceTimeP90;
    public final double serviceTimeP95;
    public final double serviceTimeP99;
    public final double serviceTimeP999;
    public final double serviceTimeP9999;
    public final double latencyP50;
    public final double latencyP90;
    public final double latencyP95;
    public final double latencyP99;
    public final double latencyP999;
    public final double latencyP9999;

    public Metrics(String operation, long successCount, long errorCount, double throughput,
                   double serviceTimeP50, double serviceTimeP90, double serviceTimeP95, double serviceTimeP99,
                   double serviceTimeP999, double serviceTimeP9999, double latencyP50, double latencyP90,
                   double latencyP95, double latencyP99, double latencyP999, double latencyP9999) {
        this.operation = operation;
        this.successCount = successCount;
        this.errorCount = errorCount;
        this.throughput = throughput;
        this.serviceTimeP50 = serviceTimeP50;
        this.serviceTimeP90 = serviceTimeP90;
        this.serviceTimeP95 = serviceTimeP95;
        this.serviceTimeP99 = serviceTimeP99;
        this.serviceTimeP999 = serviceTimeP999;
        this.serviceTimeP9999 = serviceTimeP9999;
        this.latencyP50 = latencyP50;
        this.latencyP90 = latencyP90;
        this.latencyP95 = latencyP95;
        this.latencyP99 = latencyP99;
        this.latencyP999 = latencyP999;
        this.latencyP9999 = latencyP9999;
    }
}
