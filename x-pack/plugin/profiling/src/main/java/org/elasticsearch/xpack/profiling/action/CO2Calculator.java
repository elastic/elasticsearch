/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.core.UpdateForV10;

import java.util.Map;

final class CO2Calculator {
    private static final double DEFAULT_CO2_TONS_PER_KWH = 0.000379069d; // unit: metric tons / kWh
    private static final double DEFAULT_KILOWATTS_PER_CORE_X86 = 7.0d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE_ARM64 = 2.8d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE = DEFAULT_KILOWATTS_PER_CORE_X86; // unit: watt / core
    private static final double DEFAULT_DATACENTER_PUE = 1.7d;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;
    private final double customCO2PerKWH;
    private final double customDatacenterPUE;
    private final double customKilowattsPerCoreX86;
    private final double customKilowattsPerCoreARM64;

    CO2Calculator(
        Map<String, HostMetadata> hostMetadata,
        double samplingDurationInSeconds,
        Double customCO2PerKWH,
        Double customDatacenterPUE,
        Double customPerCoreWattX86,
        Double customPerCoreWattARM64
    ) {
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds > 0 ? samplingDurationInSeconds : 1.0d; // avoid division by zero
        this.customCO2PerKWH = customCO2PerKWH == null ? DEFAULT_CO2_TONS_PER_KWH : customCO2PerKWH;
        this.customDatacenterPUE = customDatacenterPUE == null ? DEFAULT_DATACENTER_PUE : customDatacenterPUE;
        this.customKilowattsPerCoreX86 = customPerCoreWattX86 == null ? DEFAULT_KILOWATTS_PER_CORE_X86 : customPerCoreWattX86 / 1000.0d;
        this.customKilowattsPerCoreARM64 = customPerCoreWattARM64 == null
            ? DEFAULT_KILOWATTS_PER_CORE_ARM64
            : customPerCoreWattARM64 / 1000.0d;
    }

    public double getAnnualCO2Tons(String hostID, long samples, double samplingFrequency) {
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, samplingFrequency);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return DEFAULT_KILOWATTS_PER_CORE * customCO2PerKWH * annualCoreHours * customDatacenterPUE;
        }

        return getKiloWattsPerCore(host) * getCO2TonsPerKWH(host) * annualCoreHours * getDatacenterPUE(host);
    }

    @UpdateForV10(owner = UpdateForV10.Owner.PROFILING) // only allow OTEL semantic conventions
    // still required for data that has been migrated from 8.x to 9.x
    private double getKiloWattsPerCore(HostMetadata host) {
        return switch (host.hostArchitecture) {
            // For the OTEL donation of the profiling agent, we switch to OTEL semantic conventions,
            // which require "arm64" and "amd64" to be reported as the host architecture.
            case "arm64", "aarch64" -> customKilowattsPerCoreARM64;
            case "amd64", "x86_64" -> customKilowattsPerCoreX86;
            default -> DEFAULT_KILOWATTS_PER_CORE;
        };
    }

    private double getCO2TonsPerKWH(HostMetadata host) {
        return CloudProviders.getCO2TonsPerKWHOrDefault(host.instanceType.provider, host.instanceType.region, customCO2PerKWH);
    }

    private double getDatacenterPUE(HostMetadata host) {
        return CloudProviders.getPUEOrDefault(host.instanceType.provider, customDatacenterPUE);
    }
}
