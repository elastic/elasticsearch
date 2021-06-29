/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.metrics;

public final class Sample {
    private final String operation;
    private final long expectedStartTimestamp;
    private final long startTimestamp;
    private final long stopTimestamp;
    private final boolean success;

    public Sample(String operation, long expectedStartTimestamp, long startTimestamp, long stopTimestamp, boolean success) {
        this.operation = operation;
        this.expectedStartTimestamp = expectedStartTimestamp;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.success = success;
    }

    public String getOperation() {
        return operation;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public long getServiceTime() {
        return stopTimestamp - startTimestamp;
    }

    public long getLatency() {
        return stopTimestamp - expectedStartTimestamp;
    }
}
