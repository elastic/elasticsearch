/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.Collections;
import java.util.Map;

final class CO2Calculator {
    private static final double DEFAULT_SAMPLING_FREQUENCY = 20.0d;
    private static final double DEFAULT_CO2_TONS_PER_KWH = 0.000379069d; // unit: metric tons / kWh
    private static final double DEFAULT_KILOWATTS_PER_CORE_X86_64 = 7.0d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE_AARCH64 = 2.8d / 1000.0d; // unit: watt / core
    private static final double DEFAULT_KILOWATTS_PER_CORE = DEFAULT_KILOWATTS_PER_CORE_X86_64; // unit: watt / core
    private static final double DEFAULT_DATACENTER_PUE = 1.7d;
    private static final double CUSTOM_CO2_FACTOR = 1.0d;
    private static final Provider DEFAULT_PROVIDER = new Provider(DEFAULT_DATACENTER_PUE, Collections.emptyMap());
    private final CostsService costsService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;

    CO2Calculator(CostsService costsService, Map<String, HostMetadata> hostMetadata, double samplingDurationInSeconds) {
        this.costsService = costsService;
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds;
    }

    public double getAnnualCO2Tons(String hostID, long samples) {
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return DEFAULT_KILOWATTS_PER_CORE * DEFAULT_CO2_TONS_PER_KWH * annualCoreHours * DEFAULT_DATACENTER_PUE;
        }

        CostEntry costs = costsService.getCosts(host.instanceType);
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
                    Map.entry("us-east-1", 0.000379069d),
                    Map.entry("us-east-2", 0.000410608d),
                    Map.entry("us-west-1", 0.000322167d),
                    Map.entry("us-west-2", 0.000322167d),
                    Map.entry("us-gov-east-1", 0.000379069d),
                    Map.entry("us-gov-west-1", 0.000322167d),
                    Map.entry("af-south-1", 0.0009006d),
                    Map.entry("ap-east-1", 0.00071d),
                    Map.entry("ap-south-1", 0.0007082d),
                    Map.entry("ap-northeast-3", 0.0004658d),
                    Map.entry("ap-northeast-2", 0.0004156d),
                    Map.entry("ap-southeast-1", 0.000408d),
                    Map.entry("ap-southeast-2", 0.00076d),
                    Map.entry("ap-northeast-1", 0.0004658d),
                    Map.entry("ca-central-1", 0.00012d),
                    Map.entry("cn-north-1", 0.0005374d),
                    Map.entry("cn-northwest-1", 0.0005374d),
                    Map.entry("eu-central-1", 0.000311d),
                    Map.entry("eu-west-1", 0.0002786d),
                    Map.entry("eu-west-2", 0.000225d),
                    Map.entry("eu-south-1", 0.0002134d),
                    Map.entry("eu-west-3", 0.0000511d),
                    Map.entry("eu-north-1", 0.0000088d),
                    Map.entry("me-south-1", 0.0005059d),
                    Map.entry("sa-east-1", 0.0000617d)
                )
            ),
            // noinspection (explicit type arguments speedup compilation and analysis time)
            "gcp",
            new Provider(
                1.1d,
                Map.ofEntries(
                    Map.entry("us-central1", 0.00003178d),
                    Map.entry("us-east1", 0.0003504d),
                    Map.entry("us-east4", 0.00015162d),
                    Map.entry("us-west1", 0.0000078d),
                    Map.entry("us-west2", 0.00011638d),
                    Map.entry("us-west3", 0.00038376d),
                    Map.entry("us-west4", 0.00036855d),
                    Map.entry("asia-east1", 0.0004428d),
                    Map.entry("asia-east2", 0.000453d),
                    Map.entry("asia-northeast1", 0.00048752d),
                    Map.entry("asia-northeast2", 0.00048752d),
                    Map.entry("asia-northeast3", 0.00031533d),
                    Map.entry("asia-south1", 0.00063448d),
                    Map.entry("asia-south2", 0.000657d),
                    Map.entry("asia-southeast1", 0.00047328d),
                    Map.entry("asia-southeast2", 0.000647d),
                    Map.entry("australia-southeast1", 0.00064703d),
                    Map.entry("australia-southeast2", 0.000691d),
                    Map.entry("europe-central2", 0.000622d),
                    Map.entry("europe-north1", 0.00000798d),
                    Map.entry("europe-west1", 0.00004452d),
                    Map.entry("europe-west2", 0.00009471d),
                    Map.entry("europe-west3", 0.000108),
                    Map.entry("europe-west4", 0.000164d),
                    Map.entry("europe-west6", 0.000087d),
                    Map.entry("northamerica-northeast1", 0.000027d),
                    Map.entry("southamerica-east1", 0.00001236d)
                )
            ),
            "azure",
            new Provider(
                1.185d,
                Map.<String, Double>ofEntries(
                    Map.entry("Central US", 0.000426254d),
                    Map.entry("East US", 0.000379069d),
                    Map.entry("East US 2", 0.000379069d),
                    Map.entry("East US 3", 0.000379069d),
                    Map.entry("North Central US", 0.000410608d),
                    Map.entry("South Central US", 0.000373231d),
                    Map.entry("West Central US", 0.000322167d),
                    Map.entry("West US", 0.000322167d),
                    Map.entry("West US 2", 0.000322167d),
                    Map.entry("West US 3", 0.000322167d),
                    Map.entry("East Asia", 0.00071d),
                    Map.entry("Southeast Asia", 0.000408d),
                    Map.entry("South Africa North", 0.0009006d),
                    Map.entry("South Africa West", 0.0009006d),
                    Map.entry("South Africa", 0.0009006d),
                    Map.entry("Australia", 0.00079d),
                    Map.entry("Australia Central", 0.00079d),
                    Map.entry("Australia Central 2", 0.00079d),
                    Map.entry("Australia East", 0.00079d),
                    Map.entry("Australia South East", 0.00096d),
                    Map.entry("Japan", 0.0004658d),
                    Map.entry("Japan West", 0.0004658d),
                    Map.entry("Japan East", 0.0004658d),
                    Map.entry("Korea", 0.0004156d),
                    Map.entry("Korea East", 0.0004156d),
                    Map.entry("Korea South", 0.0004156d),
                    Map.entry("India", 0.0007082d),
                    Map.entry("India West", 0.0007082d),
                    Map.entry("India Central", 0.0007082d),
                    Map.entry("India South", 0.0007082d),
                    Map.entry("North Europe", 0.0002786d),
                    Map.entry("West Europe", 0.0003284d),
                    Map.entry("France", 0.00005128d),
                    Map.entry("France Central", 0.00005128d),
                    Map.entry("France South", 0.00005128d),
                    Map.entry("Sweden Central", 0.00000567d),
                    Map.entry("Switzerland", 0.00000567d),
                    Map.entry("Switzerland North", 0.00000567d),
                    Map.entry("Switzerland West", 0.00000567d),
                    Map.entry("UK", 0.000225d),
                    Map.entry("UK South", 0.000225d),
                    Map.entry("UK West", 0.000228d),
                    Map.entry("Germany", 0.00033866d),
                    Map.entry("Germany North", 0.00033866d),
                    Map.entry("Germany West Central", 0.00033866d),
                    Map.entry("Norway", 0.00000762d),
                    Map.entry("Norway East", 0.00000762d),
                    Map.entry("Norway West", 0.00000762d),
                    Map.entry("United Arab Emirates", 0.0004041d),
                    Map.entry("United Arab Emirates North", 0.0004041d),
                    Map.entry("United Arab Emirates Central", 0.0004041d),
                    Map.entry("Canada", 0.00012d),
                    Map.entry("Canada Central", 0.00012d),
                    Map.entry("Canada East", 0.00012d),
                    Map.entry("Brazil", 0.0000617d),
                    Map.entry("Brazil South", 0.0000617d),
                    Map.entry("Brazil South East", 0.0000617d)
                )
            )
        );
    }
}
