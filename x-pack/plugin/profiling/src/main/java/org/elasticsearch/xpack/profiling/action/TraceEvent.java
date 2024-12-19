/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.util.Objects;

final class TraceEvent {
    final String stacktraceID;
    final String executableName;
    final String threadName;
    final String hostID;
    long count;
    double annualCO2Tons;
    double annualCostsUSD;
    SubGroup subGroups;

    TraceEvent(String stacktraceID) {
        this(stacktraceID, "", "", "", 0);
    }

    TraceEvent(String stacktraceID, long count) {
        this(stacktraceID, "", "", "", count);
    }

    TraceEvent(String stacktraceID, String executableName, String threadName, String hostID) {
        this(stacktraceID, executableName, threadName, hostID, 0);
    }

    TraceEvent(String stacktraceID, String executableName, String threadName, String hostID, long count) {
        this.stacktraceID = stacktraceID;
        this.executableName = executableName;
        this.threadName = threadName;
        this.hostID = hostID;
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TraceEvent event = (TraceEvent) o;
        return count == event.count
            && Objects.equals(stacktraceID, event.stacktraceID)
            && Objects.equals(executableName, event.executableName)
            && Objects.equals(threadName, event.threadName)
            && Objects.equals(hostID, event.hostID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stacktraceID, executableName, threadName, hostID, count);
    }

    @Override
    public String toString() {
        return "TraceEvent{"
            + "stacktraceID='"
            + stacktraceID
            + '\''
            + ", executableName='"
            + executableName
            + '\''
            + ", threadName='"
            + threadName
            + '\''
            + ", count="
            + count
            + ", annualCO2Tons="
            + annualCO2Tons
            + ", annualCostsUSD="
            + annualCostsUSD
            + ", subGroups="
            + subGroups
            + '}';
    }
}
