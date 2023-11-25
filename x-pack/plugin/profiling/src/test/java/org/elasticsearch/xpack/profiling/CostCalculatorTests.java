/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class CostCalculatorTests extends ESTestCase {
    private static final String HOST_ID_A = "1110256254710195391";
    private static final String HOST_ID_B = "2220256254710195392";

    public void testCreateFromRegularSource() {
        InstanceTypeService instanceTypeService = new InstanceTypeService();
        instanceTypeService.load();

        // tag::noformat
        Map<String, HostMetadata> hostsTable = Map.ofEntries(
            Map.entry(HOST_ID_A,
                // known datacenter
                new HostMetadata(HOST_ID_A,
                    new InstanceType(
                        "aws",
                        "eu-west-1",
                        "c5n.xlarge"
                    ),
                    "" // Doesn't matter for cost calculation.
                )
            ),
            Map.entry(HOST_ID_B,
                new HostMetadata(HOST_ID_B,
                    // unknown datacenter
                    new InstanceType(
                        "on-prem-provider",
                        "on-prem-region",
                        "on-prem-instance-type"
                    ),
                    "" // Doesn't matter for cost calculation.
                )
            )
        );
        // end::noformat

        double samplingDurationInSeconds = 1_800.0d; // 30 minutes
        long samples = 100_000L; // 100k samples
        double annualCoreHours = CostCalculator.annualCoreHours(samplingDurationInSeconds, samples, 20.0d);
        CostCalculator costCalculator = new CostCalculator(instanceTypeService, hostsTable, samplingDurationInSeconds, null);

        // Checks whether the cost calculation is based on the pre-calculated lookup data.
        checkCostCalculation(costCalculator.annualCostsUSD(HOST_ID_A, samples), annualCoreHours, 0.061d);

        // Checks whether the cost calculation is based on the default cost factor.
        checkCostCalculation(costCalculator.annualCostsUSD(HOST_ID_B, samples), annualCoreHours, 0.0425d);
    }

    private void checkCostCalculation(double calculatedAnnualCostsUSD, double annualCoreHours, double costFactor) {
        double expectedAnnualCostsUSD = annualCoreHours * costFactor;
        assertEquals(expectedAnnualCostsUSD, calculatedAnnualCostsUSD, 0.00000001d);
    }
}
