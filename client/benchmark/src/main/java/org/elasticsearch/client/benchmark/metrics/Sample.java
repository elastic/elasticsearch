/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.metrics;

public record Sample(String operation, long expectedStartTimestamp, long startTimestamp, long stopTimestamp, boolean success) {

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
