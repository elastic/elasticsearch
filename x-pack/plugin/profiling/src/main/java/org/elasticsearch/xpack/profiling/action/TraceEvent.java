/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class TraceEvent {
    final String stacktraceID;
    double annualCO2Tons;
    double annualCostsUSD;
    long count;
    final Map<String, Long> subGroups = new HashMap<>();

    TraceEvent(String stacktraceID) {
        this(stacktraceID, 0);
    }

    TraceEvent(String stacktraceID, long count) {
        this.stacktraceID = stacktraceID;
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
        return count == event.count && Objects.equals(stacktraceID, event.stacktraceID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stacktraceID, count);
    }

    @Override
    public String toString() {
        return "TraceEvent{"
            + "stacktraceID='"
            + stacktraceID
            + '\''
            + ", annualCO2Tons="
            + annualCO2Tons
            + ", annualCostsUSD="
            + annualCostsUSD
            + ", count="
            + count
            + ", subGroups="
            + subGroups
            + '}';
    }
}
