/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

final class TraceEvent {
    long count;
    double annualCO2Tons;
    double annualCostsUSD;
    SubGroup subGroups;

    TraceEvent() {
        this(0);
    }

    TraceEvent(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "TraceEvent{"
            + "count="
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
