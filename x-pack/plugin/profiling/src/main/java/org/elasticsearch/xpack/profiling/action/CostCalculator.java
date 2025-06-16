/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.util.Map;

final class CostCalculator {
    private static final double SECONDS_PER_HOUR = 60 * 60;
    private static final double SECONDS_PER_YEAR = SECONDS_PER_HOUR * 24 * 365.0d; // unit: seconds
    public static final double DEFAULT_COST_USD_PER_CORE_HOUR = 0.0425d; // unit: USD / (core * hour)
    private static final double DEFAULT_COST_FACTOR = 1.0d;
    private static final double DEFAULT_AWS_COST_FACTOR = DEFAULT_COST_FACTOR;
    private static final double DEFAULT_AZURE_COST_FACTOR = DEFAULT_COST_FACTOR;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;
    private final double customCostPerCoreHour;
    private final Map<String, Double> providerCostFactors;

    CostCalculator(
        Map<String, HostMetadata> hostMetadata,
        double samplingDurationInSeconds,
        Double awsCostFactor,
        Double azureCostFactor,
        Double customCostPerCoreHour
    ) {
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds > 0 ? samplingDurationInSeconds : 1.0d; // avoid division by zero
        this.customCostPerCoreHour = customCostPerCoreHour == null ? DEFAULT_COST_USD_PER_CORE_HOUR : customCostPerCoreHour;
        this.providerCostFactors = Map.of(
            "aws",
            awsCostFactor == null ? DEFAULT_AWS_COST_FACTOR : awsCostFactor,
            "azure",
            azureCostFactor == null ? DEFAULT_AZURE_COST_FACTOR : azureCostFactor
        );
    }

    public double annualCostsUSD(String hostID, double samples, double samplingFrequency) {
        double annualCoreHours = annualCoreHours(samplingDurationInSeconds, samples, samplingFrequency);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return annualCoreHours * customCostPerCoreHour;
        }

        double providerCostFactor = providerCostFactors.getOrDefault(host.instanceType.provider, DEFAULT_COST_FACTOR);

        CostEntry costs = InstanceTypeService.getCosts(host.instanceType);
        if (costs == null) {
            return annualCoreHours * customCostPerCoreHour * providerCostFactor;
        }

        return annualCoreHours * (costs.usd_per_hour / host.profilingNumCores) * providerCostFactor;
    }

    public static double annualCoreHours(double duration, double samples, double samplingFrequency) {
        return (SECONDS_PER_YEAR / duration * samples / samplingFrequency) / SECONDS_PER_HOUR; // unit: core * hour
    }
}
