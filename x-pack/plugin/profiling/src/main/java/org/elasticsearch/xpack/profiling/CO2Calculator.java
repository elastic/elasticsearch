/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.Collections;
import java.util.Map;

import static java.util.Map.entry;

final class CO2Calculator {
    private static final double DEFAULT_SAMPLING_FREQUENCY = 20.0d;
    private static final double DEFAULT_CO2_TONS_PER_KWH = 0.000379069d; // unit: metric tons / kWh
    private static final double DEFAULT_KILOWATTS_PER_CORE_X86_64 = 7.0d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE_AARCH64 = 2.8d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE = DEFAULT_KILOWATTS_PER_CORE_X86_64; // unit: watt / core
    private static final double DEFAULT_DATACENTER_PUE = 1.7d;
    private static final double CUSTOM_CO2_FACTOR = 1.0d;
    private static final Provider DEFAULT_PROVIDER = new Provider(DEFAULT_DATACENTER_PUE, Collections.emptyMap());
    private final InstanceTypeService instanceTypeService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;

    CO2Calculator(InstanceTypeService instanceTypeService, Map<String, HostMetadata> hostMetadata, double samplingDurationInSeconds) {
        this.instanceTypeService = instanceTypeService;
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds;
    }

    public double getAnnualCO2Tons(String hostID, long samples) {
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return DEFAULT_KILOWATTS_PER_CORE * DEFAULT_CO2_TONS_PER_KWH * annualCoreHours * DEFAULT_DATACENTER_PUE;
        }

        CostEntry costs = instanceTypeService.getCosts(host.instanceType);
        if (costs == null) {
            return getKiloWattsPerCore(host) * getCO2TonsPerKWH(host) * annualCoreHours * getDatacenterPUE(host);
        }

        return annualCoreHours * costs.co2Factor * CUSTOM_CO2_FACTOR; // unit: metric tons
    }

    private static double getKiloWattsPerCore(HostMetadata host) {
        if ("aarch64".equals(host.profilingHostMachine)) {
            // Assume that AARCH64 (aka ARM64) machines are more energy efficient than x86_64 machines.
            return DEFAULT_KILOWATTS_PER_CORE_AARCH64;
        }
        return DEFAULT_KILOWATTS_PER_CORE;
    }

    private static double getCO2TonsPerKWH(HostMetadata host) {
        Provider provider = PROVIDERS.getOrDefault(host.instanceType.provider, DEFAULT_PROVIDER);
        return provider.co2TonsPerKWH.getOrDefault(host.instanceType.region, DEFAULT_CO2_TONS_PER_KWH);
    }

    private static double getDatacenterPUE(HostMetadata host) {
        return PROVIDERS.getOrDefault(host.instanceType.provider, DEFAULT_PROVIDER).pue;
    }

    private record Provider(double pue, Map<String, Double> co2TonsPerKWH) {}

    // values are taken from https://www.cloudcarbonfootprint.org/docs/methodology/
    private static final Map<String, Provider> PROVIDERS;
    static {
        // noinspection (explicit type arguments speedup compilation and analysis time)
        PROVIDERS = Map.of(
            "aws",
            new Provider(
                1.135d,
                Map.ofEntries(
                    entry("us-east-1", 0.000379069d),
                    entry("us-east-2", 0.000410608d),
                    entry("us-west-1", 0.000322167d),
                    entry("us-west-2", 0.000322167d),
                    entry("us-gov-east-1", 0.000379069d),
                    entry("us-gov-west-1", 0.000322167d),
                    entry("af-south-1", 0.0009006d),
                    entry("ap-east-1", 0.00071d),
                    entry("ap-south-1", 0.0007082d),
                    entry("ap-northeast-3", 0.0004658d),
                    entry("ap-northeast-2", 0.0004156d),
                    entry("ap-southeast-1", 0.000408d),
                    entry("ap-southeast-2", 0.00076d),
                    entry("ap-northeast-1", 0.0004658d),
                    entry("ca-central-1", 0.00012d),
                    entry("cn-north-1", 0.0005374d),
                    entry("cn-northwest-1", 0.0005374d),
                    entry("eu-central-1", 0.000311d),
                    entry("eu-west-1", 0.0002786d),
                    entry("eu-west-2", 0.000225d),
                    entry("eu-south-1", 0.0002134d),
                    entry("eu-west-3", 0.0000511d),
                    entry("eu-north-1", 0.0000088d),
                    entry("me-south-1", 0.0005059d),
                    entry("sa-east-1", 0.0000617d)
                )
            ),
            // noinspection (explicit type arguments speedup compilation and analysis time)
            "gcp",
            new Provider(
                1.1d,
                Map.ofEntries(
                    entry("us-central1", 0.00003178d),
                    entry("us-east1", 0.0003504d),
                    entry("us-east4", 0.00015162d),
                    entry("us-west1", 0.0000078d),
                    entry("us-west2", 0.00011638d),
                    entry("us-west3", 0.00038376d),
                    entry("us-west4", 0.00036855d),
                    entry("asia-east1", 0.0004428d),
                    entry("asia-east2", 0.000453d),
                    entry("asia-northeast1", 0.00048752d),
                    entry("asia-northeast2", 0.00048752d),
                    entry("asia-northeast3", 0.00031533d),
                    entry("asia-south1", 0.00063448d),
                    entry("asia-south2", 0.000657d),
                    entry("asia-southeast1", 0.00047328d),
                    entry("asia-southeast2", 0.000647d),
                    entry("australia-southeast1", 0.00064703d),
                    entry("australia-southeast2", 0.000691d),
                    entry("europe-central2", 0.000622d),
                    entry("europe-north1", 0.00000798d),
                    entry("europe-west1", 0.00004452d),
                    entry("europe-west2", 0.00009471d),
                    entry("europe-west3", 0.000108),
                    entry("europe-west4", 0.000164d),
                    entry("europe-west6", 0.000087d),
                    entry("northamerica-northeast1", 0.000027d),
                    entry("southamerica-east1", 0.00001236d)
                )
            ),
            "azure",
            new Provider(
                1.185d,
                Map.<String, Double>ofEntries(
                    entry("Central US", 0.000426254d),
                    entry("East US", 0.000379069d),
                    entry("East US 2", 0.000379069d),
                    entry("East US 3", 0.000379069d),
                    entry("North Central US", 0.000410608d),
                    entry("South Central US", 0.000373231d),
                    entry("West Central US", 0.000322167d),
                    entry("West US", 0.000322167d),
                    entry("West US 2", 0.000322167d),
                    entry("West US 3", 0.000322167d),
                    entry("East Asia", 0.00071d),
                    entry("Southeast Asia", 0.000408d),
                    entry("South Africa North", 0.0009006d),
                    entry("South Africa West", 0.0009006d),
                    entry("South Africa", 0.0009006d),
                    entry("Australia", 0.00079d),
                    entry("Australia Central", 0.00079d),
                    entry("Australia Central 2", 0.00079d),
                    entry("Australia East", 0.00079d),
                    entry("Australia South East", 0.00096d),
                    entry("Japan", 0.0004658d),
                    entry("Japan West", 0.0004658d),
                    entry("Japan East", 0.0004658d),
                    entry("Korea", 0.0004156d),
                    entry("Korea East", 0.0004156d),
                    entry("Korea South", 0.0004156d),
                    entry("India", 0.0007082d),
                    entry("India West", 0.0007082d),
                    entry("India Central", 0.0007082d),
                    entry("India South", 0.0007082d),
                    entry("North Europe", 0.0002786d),
                    entry("West Europe", 0.0003284d),
                    entry("France", 0.00005128d),
                    entry("France Central", 0.00005128d),
                    entry("France South", 0.00005128d),
                    entry("Sweden Central", 0.00000567d),
                    entry("Switzerland", 0.00000567d),
                    entry("Switzerland North", 0.00000567d),
                    entry("Switzerland West", 0.00000567d),
                    entry("UK", 0.000225d),
                    entry("UK South", 0.000225d),
                    entry("UK West", 0.000228d),
                    entry("Germany", 0.00033866d),
                    entry("Germany North", 0.00033866d),
                    entry("Germany West Central", 0.00033866d),
                    entry("Norway", 0.00000762d),
                    entry("Norway East", 0.00000762d),
                    entry("Norway West", 0.00000762d),
                    entry("United Arab Emirates", 0.0004041d),
                    entry("United Arab Emirates North", 0.0004041d),
                    entry("United Arab Emirates Central", 0.0004041d),
                    entry("Canada", 0.00012d),
                    entry("Canada Central", 0.00012d),
                    entry("Canada East", 0.00012d),
                    entry("Brazil", 0.0000617d),
                    entry("Brazil South", 0.0000617d),
                    entry("Brazil South East", 0.0000617d)
                )
            )
        );
    }
}
