/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.util.Map;

import static java.util.Map.entry;

final class CloudProviders {
    private CloudProviders() {
        // no instances intended
    }

    public record Provider(double pue, Map<String, Double> co2TonsPerKWH) {}

    private static Provider getProvider(String providerName) {
        return PROVIDERS.get(providerName);
    }

    /**
     * Returns the PUE for the given provider, or the default value if the provider is unknown.
     * PUE stands for Power Usage Effectiveness, and is a measure of how much power is used by
     * the datacenter infrastructure (cooling, lighting, etc.) vs. the IT equipment (servers, etc.).
     * A PUE of 1.0 means that all power is used by the IT equipment, and a PUE of 2.0 means that
     * half of the power is used by the IT equipment and half is used by the infrastructure.
     * See also https://en.wikipedia.org/wiki/Power_usage_effectiveness .
     *
     * @param providerName The name of the provider.
     *                     Currently supported providers are "aws", "gcp", and "azure".
     *                     If the provider is unknown, the default value is returned.
     * @param defaultPUEValue The default value to return if the provider is unknown.
     * @return The PUE for the given provider, or the default value if the provider is unknown.
     */
    public static double getPUEOrDefault(String providerName, double defaultPUEValue) {
        Provider provider = getProvider(providerName);
        if (provider == null) {
            return defaultPUEValue;
        }
        return provider.pue;
    }

    /**
     * Returns the CO2 emission factor for the given provider and region, or the default value if
     * the provider or region is unknown. The CO2 emission factor is the amount of CO2 emitted per
     * kWh of electricity consumed and measured in metric tons.
     *
     * @param providerName The name of the provider.
     *                     Currently supported providers are "aws", "gcp", and "azure".
     *                     If the provider is unknown, the default value is returned.
     * @param region The name of the region.
     *               If the region is unknown, the default value is returned.
     * @param defaultCO2Value The default value to return if the provider or region is unknown.
     * @return The CO2 emission factor for the given provider and region, or the default value if
     *         the provider or region is unknown.
     */
    public static double getCO2TonsPerKWHOrDefault(String providerName, String region, double defaultCO2Value) {
        Provider provider = getProvider(providerName);
        if (provider == null) {
            return defaultCO2Value;
        }
        return provider.co2TonsPerKWH.getOrDefault(region, defaultCO2Value);
    }

    // The following data taken from https://www.cloudcarbonfootprint.org/docs/methodology/
    // and updated from https://github.com/PaoloFrigo/cloud-carbon-footprint .
    // License: Apache 2.0
    // Copyright: Cloud Carbon Footprint, (C) 2021 Thoughtworks, Inc.
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
                    entry("ap-southeast-3", 0.0007177d),
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
                    entry("me-central-1", 0.0004041),
                    entry("sa-east-1", 0.0000617d)
                )
            ),
            // noinspection (explicit type arguments speedup compilation and analysis time)
            "gcp",
            new Provider(
                1.1d,
                // These emission factors take into account Google Carbon Free Energy percentage in each region.
                // Source: https://cloud.google.com/sustainability/region-carbon
                Map.ofEntries(
                    entry("us-central1", 0.0002152373529d),
                    entry("us-central2", 0.0002152373529d),
                    entry("us-east1", 0.0003255d),
                    entry("us-east4", 0.00011124d),
                    entry("us-east5", 0.00011124d),
                    entry("us-west1", 0.0000072d),
                    entry("us-west2", 0.0000893d),
                    entry("us-west3", 0.00030912d),
                    entry("us-west4", 0.00028835d),
                    entry("us-south1", 0.0001776d),
                    entry("asia-east1", 0.00037848d),
                    entry("asia-east2", 0.0002592d),
                    entry("asia-northeast1", 0.00038976d),
                    entry("asia-northeast2", 0.00026496d),
                    entry("asia-northeast3", 0.00029325d),
                    entry("asia-south1", 0.000603d),
                    entry("asia-south2", 0.00061732d),
                    entry("asia-southeast1", 0.00035712d),
                    entry("asia-southeast2", 0.0005046d),
                    entry("australia-southeast1", 0.00047242d),
                    entry("australia-southeast2", 0.00035949d),
                    entry("europe-central2", 0.0004608d),
                    entry("europe-north1", 0.00001143d),
                    entry("europe-southwest1", 0.000121d),
                    entry("europe-west1", 0.0000198d),
                    entry("europe-west2", 0.00007396d),
                    entry("europe-west3", 0.0001076),
                    entry("europe-west4", 0.00013301d),
                    entry("europe-west6", 0.0000129d),
                    entry("europe-west8", 0.000298d),
                    entry("europe-west9", 0.000059d),
                    entry("northamerica-northeast1", 0d), // Montreal is 100% CFE
                    entry("northamerica-northeast2", 0.00000232d),
                    entry("southamerica-east1", 0.00002838d),
                    entry("southamerica-west1", 0.0000589d)
                )
            ),
            "azure",
            new Provider(
                1.185d,
                Map.<String, Double>ofEntries(
                    entry("centralus", 0.000426254d),
                    entry("centraluseuap", 0.000426254d),
                    entry("centralusestage", 0.000426254d),
                    entry("eastus", 0.000379069d),
                    entry("useast", 0.000379069d),
                    entry("eastusstage", 0.000379069d),
                    entry("eastus2", 0.000379069d),
                    entry("useast2", 0.000379069d),
                    entry("eastus2euap", 0.000379069d),
                    entry("eastus2stage", 0.000379069d),
                    entry("eastus3", 0.000379069d),
                    entry("usnorth", 0.000410608d),
                    entry("northcentralus", 0.000410608d),
                    entry("northcentralusstage", 0.000410608d),
                    entry("southcentralus", 0.000373231d),
                    entry("southcentralusstage", 0.000373231d),
                    entry("unitedstates", 0.000373231d),
                    entry("unitedstateseuap", 0.000373231d),
                    entry("westcentralus", 0.000322167d),
                    entry("westcentralusstage", 0.000322167d),
                    entry("westus", 0.000322167d),
                    entry("westusstage", 0.000322167d),
                    entry("westus2", 0.000322167d),
                    entry("westus2stage", 0.000322167d),
                    entry("westus3", 0.000322167d),
                    entry("asia", 0.0005647d),
                    entry("asiapacific", 0.0005647d),
                    entry("eastasia", 0.00071d),
                    entry("eastasiastage", 0.00071d),
                    entry("southeastasia", 0.000408d),
                    entry("asiasoutheast", 0.000408d),
                    entry("southafricanorth", 0.0009006d),
                    entry("southafricawest", 0.0009006d),
                    entry("southafrica", 0.0009006d),
                    entry("australia", 0.00079d),
                    entry("australiacentral", 0.00079d),
                    entry("australiacentral2", 0.00079d),
                    entry("australiaeast", 0.00079d),
                    entry("australiasoutheast", 0.00096d),
                    entry("apeast", 0.00071d),
                    entry("apsoutheast", 0.000408d),
                    entry("japan", 0.0004658d),
                    entry("japanwest", 0.0004658d),
                    entry("japaneast", 0.0004658d),
                    entry("korea", 0.0004156d),
                    entry("koreaeast", 0.0004156d),
                    entry("koreasouth", 0.0004156d),
                    entry("india", 0.0007082d),
                    entry("indiacentral", 0.0007082d),
                    entry("centralindia", 0.0007082d),
                    entry("jioindiacentral", 0.0007082d),
                    entry("indiawest", 0.0007082d),
                    entry("westindia", 0.0007082d),
                    entry("jioindiawest", 0.0007082d),
                    entry("indiasouth", 0.0007082d),
                    entry("southindia", 0.0007082d),
                    entry("northeurope", 0.0002786d),
                    entry("europenorth", 0.0002786d),
                    entry("westeurope", 0.0003284d),
                    entry("europewest", 0.0003284d),
                    entry("france", 0.00005128d),
                    entry("francecentral", 0.00005128d),
                    entry("francesouth", 0.00005128d),
                    entry("swedencentral", 0.00000567d),
                    entry("switzerland", 0.00001152d),
                    entry("switzerlandnorth", 0.00001152d),
                    entry("switzerlandwest", 0.00001152d),
                    entry("uk", 0.000225d),
                    entry("uksouth", 0.000225d),
                    entry("ukwest", 0.000228d),
                    entry("germany", 0.00033866d),
                    entry("germanynorth", 0.00033866d),
                    entry("germanywestcentral", 0.00033866d),
                    entry("norway", 0.00000762d),
                    entry("norwayeast", 0.00000762d),
                    entry("norwaywest", 0.00000762d),
                    entry("uae", 0.0004041d),
                    entry("uaenorth", 0.0004041d),
                    entry("uaecentral", 0.0004041d),
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
