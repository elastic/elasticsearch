/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

final class TraceEvent {
    final String stacktraceID;
    double annualCO2Tons;
    double annualCostsUSD;
    long count;

    TraceEvent(String stacktraceID) {
        this.stacktraceID = stacktraceID;
    }

    TraceEvent(String stacktraceID, long count) {
        this.stacktraceID = stacktraceID;
        this.count = count;
    }
}
