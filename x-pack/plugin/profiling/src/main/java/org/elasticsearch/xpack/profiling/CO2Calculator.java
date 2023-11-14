/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.Map;

final class CO2Calculator {
    private static final double defaultSamplingFreq = 20d;
    private static final double secondsPerYear = 60d * 60d * 24d * 365d; // unit: seconds
    private static final double defaultCO2TonsPerKWH = 0.000379069d; // unit: metric tons / kWh
    private static final double defaultKiloWattsPerCore = 7d / 1000d; // unit: watt / core
    private static final double defaultDatacenterPUE = 1.7d;
    private static final double customCO2Factor = 1d;
    private final CostsService costsService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double duration;

    CO2Calculator(CostsService costsService, Map<String, HostMetadata> hostMetadata, double duration) {
        this.costsService = costsService;
        this.hostMetadata = hostMetadata;
        this.duration = duration;
    }

    public double getAnnualCO2Tons(String hostID, long samples) {
        double annualCoreHours = annualCoreHours(duration, samples, defaultSamplingFreq);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return defaultKiloWattsPerCore * defaultCO2TonsPerKWH * annualCoreHours * defaultDatacenterPUE;
        }

        CostEntry costs = costsService.getCosts(host.dci);
        if (costs == null) {
            return defaultKiloWattsPerCore * defaultCO2TonsPerKWH * annualCoreHours * defaultDatacenterPUE;
        }

        return annualCoreHours * costs.co2Factor * customCO2Factor; // unit: metric tons
    }

    private static double annualCoreHours(double duration, double samples, double samplingFreq) {
        return (secondsPerYear / duration * samples / samplingFreq) / (60 * 60); // unit: core * hour
    }
}
