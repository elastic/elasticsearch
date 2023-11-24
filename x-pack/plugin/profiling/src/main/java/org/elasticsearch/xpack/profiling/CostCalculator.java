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
    private static final double DEFAULT_CUSTOM_COST_FACTOR = 1.0d;
    private final InstanceTypeService instanceTypeService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;
    private final double customCostFactor;

    CostCalculator(
        InstanceTypeService instanceTypeService,
        Map<String, HostMetadata> hostMetadata,
        double samplingDurationInSeconds,
        Double customCostFactor
    ) {
        this.instanceTypeService = instanceTypeService;
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds > 0 ? samplingDurationInSeconds : 1.0d; // avoid division by zero
        this.customCostFactor = customCostFactor == null ? DEFAULT_CUSTOM_COST_FACTOR : customCostFactor;
    }

    public double annualCostsUSD(String hostID, double samples) {
        double annualCoreHours = annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return annualCoreHours * DEFAULT_COST_USD_PER_CORE_HOUR;
        }

        CostEntry costs = instanceTypeService.getCosts(host.instanceType);
        if (costs == null) {
            return annualCoreHours * DEFAULT_COST_USD_PER_CORE_HOUR;
        }

        return annualCoreHours * costs.costFactor * customCostFactor;
    }

    public static double annualCoreHours(double duration, double samples, double samplingFrequency) {
        // samplingFrequency will a variable value when we start supporting probabilistic profiling (soon).
        return (SECONDS_PER_YEAR / duration * samples / samplingFrequency) / SECONDS_PER_HOUR; // unit: core * hour
    }
}
