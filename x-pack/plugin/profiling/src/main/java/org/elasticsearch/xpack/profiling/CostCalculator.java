/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.Map;

final class CostCalculator {
    private static final double defaultSamplingFreq = 20d;
    private static final double secondsPerYear = 60d * 60d * 24d * 365d; // unit: seconds
    private static final double defaultCostUSDPerCoreHour = 0.0425d; // unit: USD / (core * hour)
    private static final double customCostFactor = 1d;
    private final CostsService costsService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double duration;

    CostCalculator(CostsService costsService, Map<String, HostMetadata> hostMetadata, double duration) {
        this.costsService = costsService;
        this.hostMetadata = hostMetadata;
        this.duration = duration;
    }

    public double annualCostsUSD(String hostID, double samples) {
        double annualCoreHours = annualCoreHours(duration, samples, defaultSamplingFreq);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return annualCoreHours * defaultCostUSDPerCoreHour;
        }

        CostEntry costs = costsService.getCosts(host.dci);
        if (costs == null) {
            return annualCoreHours * defaultCostUSDPerCoreHour;
        }

        return annualCoreHours * costs.costFactor * customCostFactor;
    }

    private static double annualCoreHours(double duration, double samples, double samplingFreq) {
        return (secondsPerYear / duration * samples / samplingFreq) / (60 * 60); // unit: core * hour
    }
}
