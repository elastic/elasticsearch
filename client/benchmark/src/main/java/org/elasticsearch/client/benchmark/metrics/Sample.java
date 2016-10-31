/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
