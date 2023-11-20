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

public class CO2CalculatorTests extends ESTestCase {
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
                // known datacenter and instance type
                new HostMetadata(hostIDA,
                    new DatacenterInstance(
                        "aws",
                        "eu-west-1",
                        "c5n.xlarge"
                    ),
                    "" // Doesn't matter if datacenter is known.
                )
            ),
            new AbstractMap.SimpleEntry<>(hostIDB,
                new HostMetadata(hostIDB,
                    // unknown datacenter, known provider and region, x86_64
                    new DatacenterInstance(
                        "gcp",
                        "europe-west1",
                        "" // Doesn't matter for unknown datacenters.
                    ),
                    "x86_64"
                )
            ),
            new AbstractMap.SimpleEntry<>(hostIDC,
                new HostMetadata(hostIDC,
                    // unknown datacenter, known provider and region, aarch64
                    new DatacenterInstance(
                        "azure",
                        "North Central US",
                        "" // Doesn't matter for unknown datacenters.
                    ),
                    "aarch64"
                )
            ),
            new AbstractMap.SimpleEntry<>(hostIDD,
                new HostMetadata(hostIDD,
                    // unknown datacenter, unknown provider and region, aarch64
                    new DatacenterInstance(
                        "on-prem-provider",
                        "on-prem-region",
                        "" // Doesn't matter for unknown datacenters.
                    ),
                    "aarch64"
                )
            )
        );
        // end::noformat

        double duration = 1_800d; // 30 minutes
        long samples = 100_000L; // 100k samples
        double annualCoreHours = CostCalculator.annualCoreHours(duration, samples, 20d);
        CO2Calculator co2Calculator = new CO2Calculator(costsService, hostsTable, duration);

        checkCO2Calculation(co2Calculator.getAnnualCO2Tons(hostIDA, samples), annualCoreHours, 0.000002213477d);
        checkCO2Calculation(co2Calculator.getAnnualCO2Tons(hostIDB, samples), annualCoreHours, 1.1d, 0.00004452d, 7d);
        checkCO2Calculation(co2Calculator.getAnnualCO2Tons(hostIDC, samples), annualCoreHours, 1.185d, 0.000410608d, 2.8d);
        checkCO2Calculation(co2Calculator.getAnnualCO2Tons(hostIDD, samples), annualCoreHours, 1.7d, 0.000379069d, 2.8d);
    }

    private void checkCO2Calculation(double calculatedAnnualCO2Tons, double annualCoreHours, double co2Factor) {
        double expectedAnnualCO2Tons = annualCoreHours * co2Factor;
        assertEquals(expectedAnnualCO2Tons, calculatedAnnualCO2Tons, 0.000000000001d);
    }

    private void checkCO2Calculation(
        double calculatedAnnualCO2Tons,
        double annualCoreHours,
        double datacenterPUE,
        double co2TonsPerKWH,
        double wattsPerCore
    ) {
        double kiloWattsPerCore = wattsPerCore / 1000d;
        double expectedAnnualCO2Tons = annualCoreHours * datacenterPUE * co2TonsPerKWH * kiloWattsPerCore;
        assertEquals(expectedAnnualCO2Tons, calculatedAnnualCO2Tons, 0.000000000001d);
    }
}
