/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.core.UpdateForV9;
import java.util.Map;
import java.util.logging.Logger;

final class CO2Calculator {
    private static final Logger logger = Logger.getLogger(CO2Calculator.class.getName());

    private static final double DEFAULT_SAMPLING_FREQUENCY = 20.0d;
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
        // Avoid division by zero by ensuring sampling duration is greater than zero
        this.samplingDurationInSeconds = samplingDurationInSeconds > 0 ? samplingDurationInSeconds : 1.0d;
        // Use default values if custom parameters are not provided
        this.customCO2PerKWH = customCO2PerKWH == null ? DEFAULT_CO2_TONS_PER_KWH : customCO2PerKWH;
        this.customDatacenterPUE = customDatacenterPUE == null ? DEFAULT_DATACENTER_PUE : customDatacenterPUE;
        this.customKilowattsPerCoreX86 = customPerCoreWattX86 == null ? DEFAULT_KILOWATTS_PER_CORE_X86 : customPerCoreWattX86 / 1000.0d;
        this.customKilowattsPerCoreARM64 = customPerCoreWattARM64 == null
            ? DEFAULT_KILOWATTS_PER_CORE_ARM64
            : customPerCoreWattARM64 / 1000.0d;
    }

    /**
     * Calculates the annual CO2 emissions in metric tons for a given host and sample count.
     *
     * @param hostID The ID of the host
     * @param samples The number of samples
     * @return The annual CO2 emissions in metric tons
     */
    public double getAnnualCO2Tons(String hostID, long samples) {
        // Calculate annual core hours based on sampling duration and frequency
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        // Retrieve host metadata for the given hostID
        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            logger.warning("Host metadata is null for hostID: " + hostID);
            // Use default values if host metadata is not available
            return DEFAULT_KILOWATTS_PER_CORE * customCO2PerKWH * annualCoreHours * customDatacenterPUE;
        }

        // Calculate CO2 emissions using host-specific metadata
        return getKiloWattsPerCore(host) * getCO2TonsPerKWH(host) * annualCoreHours * getDatacenterPUE(host);
    }

    /**
     * Retrieves the power consumption per core for the given host based on its architecture.
     *
     * @param host The host metadata
     * @return The power consumption per core in kW
     */
    @UpdateForV9 // only allow OTEL semantic conventions
    private double getKiloWattsPerCore(HostMetadata host) {
        if (host == null || host.hostArchitecture == null) {
            logger.warning("Host or host architecture is null for host: " + host);
            return DEFAULT_KILOWATTS_PER_CORE;
        }

        return switch (host.hostArchitecture) {
            // For OTEL semantic conventions, "arm64" and "amd64" are reported as the host architecture
            case "arm64", "aarch64" -> customKilowattsPerCoreARM64;
            case "amd64", "x86_64" -> customKilowattsPerCoreX86;
            default -> {
                logger.warning("Unknown host architecture: " + host.hostArchitecture);
                yield DEFAULT_KILOWATTS_PER_CORE;
            }
        };
    }


    /**
     * Retrieves the CO2 emission factor for the given host based on its cloud provider and region.
     *
     * @param host The host metadata
     * @return The CO2 emission factor in metric tons per kWh
     */
    private double getCO2TonsPerKWH(HostMetadata host) {
        if (host.instanceType == null || host.instanceType.provider == null || host.instanceType.region == null) {
            logger.warning("Incomplete instance type information for host: " + host);
            return customCO2PerKWH;
        }
        return CloudProviders.getCO2TonsPerKWHOrDefault(host.instanceType.provider, host.instanceType.region, customCO2PerKWH);
    }

    /**
     * Retrieves the Power Usage Effectiveness (PUE) of the datacenter for the given host.
     *
     * @param host The host metadata
     * @return The PUE of the datacenter
     */
    private double getDatacenterPUE(HostMetadata host) {
        if (host.instanceType == null || host.instanceType.provider == null) {
            logger.warning("Incomplete instance type information for host: " + host);
            return customDatacenterPUE;
        }
        return CloudProviders.getPUEOrDefault(host.instanceType.provider, customDatacenterPUE);
    }
}
