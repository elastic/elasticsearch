/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class CostCalculatorTests extends ESTestCase {
    private static final String HOST_ID_AWS = "1110256254710195391";
    private static final int HOST_ID_A_NUM_CORES = 8;
    private static final String HOST_ID_AZURE = "2220256254710195392";
    private static final int HOST_ID_AZURE_NUM_CORES = 8;
    private static final String HOST_ID_UNKNOWN = "3330256254710195392";
    private static final Integer HOST_ID_UNKNOWN_NUM_CORES = null; // number of cores are unknown

    public void testCreateFromRegularSource() {
        // tag::noformat
        Map<String, HostMetadata> hostsTable = Map.ofEntries(
            Map.entry(HOST_ID_AWS,
                // known datacenter
                new HostMetadata(HOST_ID_AWS,
                    new InstanceType(
                        "aws",
                        "eu-west-1",
                        "c5n.xlarge"
                    ),
                    "", // Doesn't matter for cost calculation.
                    HOST_ID_A_NUM_CORES // number of cores
                )
            ),
            Map.entry(HOST_ID_AZURE,
                // known datacenter
                new HostMetadata(HOST_ID_AZURE,
                    new InstanceType(
                        "azure",
                        "eastus2",
                        "Standard_D4s_v3"
                    ),
                    "", // Doesn't matter for cost calculation.
                    HOST_ID_AZURE_NUM_CORES // number of cores
                )
            ),
            Map.entry(HOST_ID_UNKNOWN,
                new HostMetadata(HOST_ID_UNKNOWN,
                    // unknown datacenter
                    new InstanceType(
                        "on-prem-provider",
                        "on-prem-region",
                        "on-prem-instance-type"
                    ),
                    "", // Doesn't matter for cost calculation.
                    HOST_ID_UNKNOWN_NUM_CORES // number of cores
                )
            )
        );
        // end::noformat

        double samplingDurationInSeconds = 1_800.0d; // 30 minutes
        long samples = 100_000L; // 100k samples
        double defaultFreq = TransportGetStackTracesAction.DEFAULT_SAMPLING_FREQUENCY;

        // Plausibility check with different frequency factors.
        // 10x higher frequency means 10x less annualCoreHours with the same number of samples,
        // and also 10x lower cost.
        for (double freq : new double[] { defaultFreq, defaultFreq * 10 }) {
            // Calculate the annual core hours based on the sampling duration, samples, and frequency.
            double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, freq);

            // Create a new CostCalculator instance with the hosts table and sampling duration.
            CostCalculator costCalculator = new CostCalculator(hostsTable, samplingDurationInSeconds, null, null, null);

            // Check the cost calculation for AWS host ID.
            checkCostCalculation(costCalculator.annualCostsUSD(HOST_ID_AWS, samples, freq), annualCoreHours, 0.244d, HOST_ID_A_NUM_CORES);

            // Check the cost calculation for Azure host ID.
            checkCostCalculation(
                costCalculator.annualCostsUSD(HOST_ID_AZURE, samples, freq),
                annualCoreHours,
                0.192d,
                HOST_ID_AZURE_NUM_CORES
            );

            // Check the cost calculation for unknown host ID.
            checkCostCalculation(
                costCalculator.annualCostsUSD(HOST_ID_UNKNOWN, samples, freq),
                annualCoreHours,
                CostCalculator.DEFAULT_COST_USD_PER_CORE_HOUR * HostMetadata.DEFAULT_PROFILING_NUM_CORES,
                HostMetadata.DEFAULT_PROFILING_NUM_CORES
            );
        }
    }

    private void checkCostCalculation(double calculatedAnnualCostsUSD, double annualCoreHours, double usd_per_hour, int profilingNumCores) {
        double expectedAnnualCostsUSD = annualCoreHours * (usd_per_hour / profilingNumCores);
        assertEquals(expectedAnnualCostsUSD, calculatedAnnualCostsUSD, 0.00000001d);
    }
}
