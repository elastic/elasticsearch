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
    private static final Provider DEFAULT_PROVIDER = new Provider(DEFAULT_DATACENTER_PUE, Collections.emptyMap());
    private final InstanceTypeService instanceTypeService;
    private final Map<String, HostMetadata> hostMetadata;
    private final double samplingDurationInSeconds;
    private final double customCO2PerKWH;
    private final double customDatacenterPUE;
    private final double customKilowattsPerCore;

    CO2Calculator(
        InstanceTypeService instanceTypeService,
        Map<String, HostMetadata> hostMetadata,
        double samplingDurationInSeconds,
        Double customCO2PerKWH,
        Double customDatacenterPUE,
        Double customPerCoreWatt
    ) {
        this.instanceTypeService = instanceTypeService;
        this.hostMetadata = hostMetadata;
        this.samplingDurationInSeconds = samplingDurationInSeconds > 0 ? samplingDurationInSeconds : 1.0d; // avoid division by zero
        this.customCO2PerKWH = customCO2PerKWH == null ? DEFAULT_CO2_TONS_PER_KWH : customCO2PerKWH;
        this.customDatacenterPUE = customDatacenterPUE == null ? DEFAULT_DATACENTER_PUE : customDatacenterPUE;
        this.customKilowattsPerCore = customPerCoreWatt == null ? DEFAULT_KILOWATTS_PER_CORE : customPerCoreWatt / 1000.0d;
    }

    public double getAnnualCO2Tons(String hostID, long samples) {
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, DEFAULT_SAMPLING_FREQUENCY);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null) {
            return customKilowattsPerCore * customCO2PerKWH * annualCoreHours * customDatacenterPUE;
        }

        CostEntry costs = instanceTypeService.getCosts(host.instanceType);
        if (costs == null) {
            return getKiloWattsPerCore(host) * getCO2TonsPerKWH(host) * annualCoreHours * getDatacenterPUE(host);
        }

        return annualCoreHours * costs.co2Factor; // unit: metric tons
    }

    private double getKiloWattsPerCore(HostMetadata host) {
        if ("aarch64".equals(host.profilingHostMachine)) {
            // Assume that AARCH64 (aka ARM64) machines are more energy efficient than x86_64 machines.
            return DEFAULT_KILOWATTS_PER_CORE_AARCH64;
        }
        return customKilowattsPerCore;
    }

    private double getCO2TonsPerKWH(HostMetadata host) {
        Provider provider = PROVIDERS.getOrDefault(host.instanceType.provider, DEFAULT_PROVIDER);
        return provider.co2TonsPerKWH.getOrDefault(host.instanceType.region, customCO2PerKWH);
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
                    entry("centralus", 0.000426254d),
                    entry("eastus", 0.000379069d),
                    entry("eastus2", 0.000379069d),
                    entry("eastus3", 0.000379069d),
                    entry("northcentralus", 0.000410608d),
                    entry("southcentralus", 0.000373231d),
                    entry("westcentralusS", 0.000322167d),
                    entry("westus", 0.000322167d),
                    entry("westus2", 0.000322167d),
                    entry("westus3", 0.000322167d),
                    entry("eastasia", 0.00071d),
                    entry("southeastasia", 0.000408d),
                    entry("southafricanorth", 0.0009006d),
                    entry("southafricawest", 0.0009006d),
                    entry("southafrica", 0.0009006d),
                    entry("australia", 0.00079d),
                    entry("australiacentral", 0.00079d),
                    entry("australiacentral2", 0.00079d),
                    entry("australiaeast", 0.00079d),
                    entry("australiasoutheast", 0.00096d),
                    entry("japan", 0.0004658d),
                    entry("japanwest", 0.0004658d),
                    entry("japaneast", 0.0004658d),
                    entry("korea", 0.0004156d),
                    entry("koreaeast", 0.0004156d),
                    entry("koreasouth", 0.0004156d),
                    entry("india", 0.0007082d),
                    entry("indiawest", 0.0007082d),
                    entry("indiacentral", 0.0007082d),
                    entry("indiasouth", 0.0007082d),
                    entry("northeurope", 0.0002786d),
                    entry("westeurope", 0.0003284d),
                    entry("france", 0.00005128d),
                    entry("francecentral", 0.00005128d),
                    entry("francesouth", 0.00005128d),
                    entry("swedencentral", 0.00000567d),
                    entry("switzerland", 0.00000567d),
                    entry("switzerlandnorth", 0.00000567d),
                    entry("switzerlandwest", 0.00000567d),
                    entry("uk", 0.000225d),
                    entry("uksouth", 0.000225d),
                    entry("ukwest", 0.000228d),
                    entry("germany", 0.00033866d),
                    entry("germanynorth", 0.00033866d),
                    entry("germanywestcentral", 0.00033866d),
                    entry("norway", 0.00000762d),
                    entry("norwayeast", 0.00000762d),
                    entry("norwaywest", 0.00000762d),
                    entry("unitedarabemirates", 0.0004041d),
                    entry("unitedarabemiratesnorth", 0.0004041d),
                    entry("unitedarabemiratescentral", 0.0004041d),
                    entry("canada", 0.00012d),
                    entry("canadacentral", 0.00012d),
                    entry("canadaeast", 0.00012d),
                    entry("brazil", 0.0000617d),
                    entry("brazilsouth", 0.0000617d),
                    entry("brazilsoutheast", 0.0000617d)
                )
            )
        );
    }
}
