/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.Map;

final class CostCalculator {
    private static final double DEFAULT_SAMPLING_FREQUENCY = 20.0d;
    private static final double SECONDS_PER_HOUR = 60 * 60;
    private static final double SECONDS_PER_YEAR = SECONDS_PER_HOUR * 24 * 365.0d; // unit: seconds
    private static final double DEFAULT_COST_USD_PER_CORE_HOUR = 0.0425d; // unit: USD / (core * hour)
    private static final double CUSTOM_COST_FACTOR = 1.0d;
    private final CostsService costsService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;

    CostCalculator(CostsService costsService, Map<String, HostMetadata> hostMetadata, double samplingDurationInSeconds) {
        this.costsService = costsService;
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds;
    }

    public double annualCostsUSD(String hostID, double samples) {
        double annualCoreHours = annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return annualCoreHours * DEFAULT_COST_USD_PER_CORE_HOUR;
        }

        CostEntry costs = costsService.getCosts(host.instanceType);
        if (costs == null) {
            return annualCoreHours * DEFAULT_COST_USD_PER_CORE_HOUR;
        }

        return annualCoreHours * costs.costFactor * CUSTOM_COST_FACTOR;
    }

    public static double annualCoreHours(double duration, double samples, double samplingFrequency) {
        // samplingFrequency will a variable value when we start supporting probabilistic profiling (soon).
        return (SECONDS_PER_YEAR / duration * samples / samplingFrequency) / SECONDS_PER_HOUR; // unit: core * hour
    }
}
