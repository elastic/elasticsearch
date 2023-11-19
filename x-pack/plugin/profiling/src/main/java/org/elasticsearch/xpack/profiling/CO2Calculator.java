/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class CO2Calculator {
    private static final double defaultSamplingFreq = 20d;
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
        double annualCoreHours = CostCalculator.annualCoreHours(duration, samples, defaultSamplingFreq);

        HostMetadata host = hostMetadata.get(hostID);
        if (host == null || host.isEmpty()) {
            return defaultKiloWattsPerCore * defaultCO2TonsPerKWH * annualCoreHours * defaultDatacenterPUE;
        }

        CostEntry costs = costsService.getCosts(host.dci);
        if (costs == null) {
            return getKiloWattsPerCore(host) * getCO2TonsPerKWH(host) * annualCoreHours * getDatacenterPUE(host);
        }

        return annualCoreHours * costs.co2Factor * customCO2Factor; // unit: metric tons
    }

    private static double getKiloWattsPerCore(HostMetadata host) {
        if (Objects.equals(host.profilingHostMachine, "aarch64")) {
            return 2.8d / 1000d;
        }
        return defaultKiloWattsPerCore;
    }

    private static double getCO2TonsPerKWH(HostMetadata host) {
        Provider provider = providers.get(host.dci.provider);
        if (provider == null || provider.co2TonsPerKWH.containsKey(host.dci.region) == false) {
            return defaultCO2TonsPerKWH;
        }
        return provider.co2TonsPerKWH.get(host.dci.region);
    }

    private static double getDatacenterPUE(HostMetadata host) {
        Provider provider = providers.get(host.dci.provider);
        if (provider == null) {
            return defaultDatacenterPUE;
        }
        return provider.PUE;
    }

    private static class Provider {
        double PUE;
        Map<String, Double> co2TonsPerKWH;

        Provider(double PUE, Map<String, Double> co2TonsPerKWH) {
            this.PUE = PUE;
            this.co2TonsPerKWH = co2TonsPerKWH;
        }
    }

    // values are taken from https://www.cloudcarbonfootprint.org/docs/methodology/
    private static final Map<String, Provider> providers = new HashMap<>();
    static {
        // noinspection (explicit type arguments speedup compilation and analysis time)
        providers.put(
            "aws",
            new Provider(
                1.135d,
                Map.ofEntries(
                    new AbstractMap.SimpleEntry<>("us-east-1", 0.000379069d),
                    new AbstractMap.SimpleEntry<>("us-east-2", 0.000410608d),
                    new AbstractMap.SimpleEntry<>("us-west-1", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("us-west-2", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("us-gov-east-1", 0.000379069d),
                    new AbstractMap.SimpleEntry<>("us-gov-west-1", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("af-south-1", 0.0009006d),
                    new AbstractMap.SimpleEntry<>("ap-east-1", 0.00071d),
                    new AbstractMap.SimpleEntry<>("ap-south-1", 0.0007082d),
                    new AbstractMap.SimpleEntry<>("ap-northeast-3", 0.0004658d),
                    new AbstractMap.SimpleEntry<>("ap-northeast-2", 0.0004156d),
                    new AbstractMap.SimpleEntry<>("ap-southeast-1", 0.000408d),
                    new AbstractMap.SimpleEntry<>("ap-southeast-2", 0.00076d),
                    new AbstractMap.SimpleEntry<>("ap-northeast-1", 0.0004658d),
                    new AbstractMap.SimpleEntry<>("ca-central-1", 0.00012d),
                    new AbstractMap.SimpleEntry<>("cn-north-1", 0.0005374d),
                    new AbstractMap.SimpleEntry<>("cn-northwest-1", 0.0005374d),
                    new AbstractMap.SimpleEntry<>("eu-central-1", 0.000311d),
                    new AbstractMap.SimpleEntry<>("eu-west-1", 0.0002786d),
                    new AbstractMap.SimpleEntry<>("eu-west-2", 0.000225d),
                    new AbstractMap.SimpleEntry<>("eu-south-1", 0.0002134d),
                    new AbstractMap.SimpleEntry<>("eu-west-3", 0.0000511d),
                    new AbstractMap.SimpleEntry<>("eu-north-1", 0.0000088d),
                    new AbstractMap.SimpleEntry<>("me-south-1", 0.0005059d),
                    new AbstractMap.SimpleEntry<>("sa-east-1", 0.0000617d)
                )
            )
        );
        // noinspection (explicit type arguments speedup compilation and analysis time)
        providers.put(
            "gcp",
            new Provider(
                1.1d,
                Map.ofEntries(
                    new AbstractMap.SimpleEntry<>("us-central1", 0.00003178d),
                    new AbstractMap.SimpleEntry<>("us-east1", 0.0003504d),
                    new AbstractMap.SimpleEntry<>("us-east4", 0.00015162d),
                    new AbstractMap.SimpleEntry<>("us-west1", 0.0000078d),
                    new AbstractMap.SimpleEntry<>("us-west2", 0.00011638d),
                    new AbstractMap.SimpleEntry<>("us-west3", 0.00038376d),
                    new AbstractMap.SimpleEntry<>("us-west4", 0.00036855d),
                    new AbstractMap.SimpleEntry<>("asia-east1", 0.0004428d),
                    new AbstractMap.SimpleEntry<>("asia-east2", 0.000453d),
                    new AbstractMap.SimpleEntry<>("asia-northeast1", 0.00048752d),
                    new AbstractMap.SimpleEntry<>("asia-northeast2", 0.00048752d),
                    new AbstractMap.SimpleEntry<>("asia-northeast3", 0.00031533d),
                    new AbstractMap.SimpleEntry<>("asia-south1", 0.00063448d),
                    new AbstractMap.SimpleEntry<>("asia-south2", 0.000657d),
                    new AbstractMap.SimpleEntry<>("asia-southeast1", 0.00047328d),
                    new AbstractMap.SimpleEntry<>("asia-southeast2", 0.000647d),
                    new AbstractMap.SimpleEntry<>("australia-southeast1", 0.00064703d),
                    new AbstractMap.SimpleEntry<>("australia-southeast2", 0.000691d),
                    new AbstractMap.SimpleEntry<>("europe-central2", 0.000622d),
                    new AbstractMap.SimpleEntry<>("europe-north1", 0.00000798d),
                    new AbstractMap.SimpleEntry<>("europe-west1", 0.00004452d),
                    new AbstractMap.SimpleEntry<>("europe-west2", 0.00009471d),
                    new AbstractMap.SimpleEntry<>("europe-west3", 0.000108),
                    new AbstractMap.SimpleEntry<>("europe-west4", 0.000164d),
                    new AbstractMap.SimpleEntry<>("europe-west6", 0.000087d),
                    new AbstractMap.SimpleEntry<>("northamerica-northeast1", 0.000027d),
                    new AbstractMap.SimpleEntry<>("southamerica-east1", 0.00001236d)
                )
            )
        );
        // noinspection (explicit type arguments speedup compilation and analysis time)
        providers.put(
            "azure",
            new Provider(
                1.185d,
                Map.<String, Double>ofEntries(
                    new AbstractMap.SimpleEntry<>("Central US", 0.000426254d),
                    new AbstractMap.SimpleEntry<>("East US", 0.000379069d),
                    new AbstractMap.SimpleEntry<>("East US 2", 0.000379069d),
                    new AbstractMap.SimpleEntry<>("East US 3", 0.000379069d),
                    new AbstractMap.SimpleEntry<>("North Central US", 0.000410608d),
                    new AbstractMap.SimpleEntry<>("South Central US", 0.000373231d),
                    new AbstractMap.SimpleEntry<>("West Central US", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("West US", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("West US 2", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("West US 3", 0.000322167d),
                    new AbstractMap.SimpleEntry<>("East Asia", 0.00071d),
                    new AbstractMap.SimpleEntry<>("Southeast Asia", 0.000408d),
                    new AbstractMap.SimpleEntry<>("South Africa North", 0.0009006d),
                    new AbstractMap.SimpleEntry<>("South Africa West", 0.0009006d),
                    new AbstractMap.SimpleEntry<>("South Africa", 0.0009006d),
                    new AbstractMap.SimpleEntry<>("Australia", 0.00079d),
                    new AbstractMap.SimpleEntry<>("Australia Central", 0.00079d),
                    new AbstractMap.SimpleEntry<>("Australia Central 2", 0.00079d),
                    new AbstractMap.SimpleEntry<>("Australia East", 0.00079d),
                    new AbstractMap.SimpleEntry<>("Australia South East", 0.00096d),
                    new AbstractMap.SimpleEntry<>("Japan", 0.0004658d),
                    new AbstractMap.SimpleEntry<>("Japan West", 0.0004658d),
                    new AbstractMap.SimpleEntry<>("Japan East", 0.0004658d),
                    new AbstractMap.SimpleEntry<>("Korea", 0.0004156d),
                    new AbstractMap.SimpleEntry<>("Korea East", 0.0004156d),
                    new AbstractMap.SimpleEntry<>("Korea South", 0.0004156d),
                    new AbstractMap.SimpleEntry<>("India", 0.0007082d),
                    new AbstractMap.SimpleEntry<>("India West", 0.0007082d),
                    new AbstractMap.SimpleEntry<>("India Central", 0.0007082d),
                    new AbstractMap.SimpleEntry<>("India South", 0.0007082d),
                    new AbstractMap.SimpleEntry<>("North Europe", 0.0002786d),
                    new AbstractMap.SimpleEntry<>("West Europe", 0.0003284d),
                    new AbstractMap.SimpleEntry<>("France", 0.00005128d),
                    new AbstractMap.SimpleEntry<>("France Central", 0.00005128d),
                    new AbstractMap.SimpleEntry<>("France South", 0.00005128d),
                    new AbstractMap.SimpleEntry<>("Sweden Central", 0.00000567d),
                    new AbstractMap.SimpleEntry<>("Switzerland", 0.00000567d),
                    new AbstractMap.SimpleEntry<>("Switzerland North", 0.00000567d),
                    new AbstractMap.SimpleEntry<>("Switzerland West", 0.00000567d),
                    new AbstractMap.SimpleEntry<>("UK", 0.000225d),
                    new AbstractMap.SimpleEntry<>("UK South", 0.000225d),
                    new AbstractMap.SimpleEntry<>("UK West", 0.000228d),
                    new AbstractMap.SimpleEntry<>("Germany", 0.00033866d),
                    new AbstractMap.SimpleEntry<>("Germany North", 0.00033866d),
                    new AbstractMap.SimpleEntry<>("Germany West Central", 0.00033866d),
                    new AbstractMap.SimpleEntry<>("Norway", 0.00000762d),
                    new AbstractMap.SimpleEntry<>("Norway East", 0.00000762d),
                    new AbstractMap.SimpleEntry<>("Norway West", 0.00000762d),
                    new AbstractMap.SimpleEntry<>("United Arab Emirates", 0.0004041d),
                    new AbstractMap.SimpleEntry<>("United Arab Emirates North", 0.0004041d),
                    new AbstractMap.SimpleEntry<>("United Arab Emirates Central", 0.0004041d),
                    new AbstractMap.SimpleEntry<>("Canada", 0.00012d),
                    new AbstractMap.SimpleEntry<>("Canada Central", 0.00012d),
                    new AbstractMap.SimpleEntry<>("Canada East", 0.00012d),
                    new AbstractMap.SimpleEntry<>("Brazil", 0.0000617d),
                    new AbstractMap.SimpleEntry<>("Brazil South", 0.0000617d),
                    new AbstractMap.SimpleEntry<>("Brazil South East", 0.0000617d)
                )
            )
        );
    }
}
