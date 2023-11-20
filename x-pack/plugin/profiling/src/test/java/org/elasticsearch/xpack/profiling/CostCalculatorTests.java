/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

import java.util.AbstractMap;
import java.util.Map;

public class CostCalculatorTests extends ESTestCase {
    private static final String hostIDA = "1110256254710195391";
    private static final String hostIDB = "2220256254710195392";
    private static final String hostIDC = "3330256254710195393";
    private static final String hostIDD = "4440256254710195394";

    public void testCreateFromRegularSource() {
        CostsService costsService = new CostsService();
        costsService.load();

        // tag::noformat
        Map<String, HostMetadata> hostsTable = Map.ofEntries(
            new AbstractMap.SimpleEntry<>(hostIDA,
                // known datacenter
                new HostMetadata(hostIDA,
                    new DatacenterInstance(
                        "aws",
                        "eu-west-1",
                        "c5n.xlarge"
                    ),
                    "" // Doesn't matter for cost calculation.
                )
            ),
            new AbstractMap.SimpleEntry<>(hostIDB,
                new HostMetadata(hostIDB,
                    // unknown datacenter
                    new DatacenterInstance(
                        "on-prem-provider",
                        "on-prem-region",
                        "on-prem-instance-type"
                    ),
                    "" // Doesn't matter for cost calculation.
                )
            )
        );
        // end::noformat

        double duration = 1_800d; // 30 minutes
        long samples = 100_000L; // 100k samples
        double annualCoreHours = CostCalculator.annualCoreHours(duration, samples, 20d);
        CostCalculator costCalculator = new CostCalculator(costsService, hostsTable, duration);

        // Checks whether the cost calculation is based on the pre-calculated lookup data.
        checkCostCalculation(costCalculator.annualCostsUSD(hostIDA, samples), annualCoreHours, 0.061d);

        // Checks whether the cost calculation is based on the default cost factor.
        checkCostCalculation(costCalculator.annualCostsUSD(hostIDB, samples), annualCoreHours, 0.0425d);
    }

    private void checkCostCalculation(double calculatedAnnualCostsUSD, double annualCoreHours, double costFactor) {
        double expectedAnnualCostsUSD = annualCoreHours * costFactor;
        assertEquals(expectedAnnualCostsUSD, calculatedAnnualCostsUSD, 0.00000001d);
    }
}
