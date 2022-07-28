/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HealthIndicatorServiceTests extends ESTestCase {

    public void testImpactsOrderAndLimit() {
        // This test adds 23 impacts, and asserts that only the highest-severity 3 come back from HealthIndicatorService.createIndicator()
        HealthIndicatorService service = getTestHealthIndicatorService();
        HealthStatus status = randomFrom(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN);
        Set<HealthIndicatorImpact> impacts = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            impacts.add(
                new HealthIndicatorImpact(randomIntBetween(5, 20), randomAlphaOfLength(20), List.of(randomFrom(ImpactArea.values())))
            );
        }
        HealthIndicatorImpact impact1 = new HealthIndicatorImpact(1, randomAlphaOfLength(20), List.of(randomFrom(ImpactArea.values())));
        HealthIndicatorImpact impact2 = new HealthIndicatorImpact(2, randomAlphaOfLength(20), List.of(randomFrom(ImpactArea.values())));
        HealthIndicatorImpact impact3 = new HealthIndicatorImpact(3, randomAlphaOfLength(20), List.of(randomFrom(ImpactArea.values())));
        impacts.add(impact2);
        impacts.add(impact1);
        impacts.add(impact3);
        for (int i = 0; i < 10; i++) {
            impacts.add(
                new HealthIndicatorImpact(randomIntBetween(5, 20), randomAlphaOfLength(20), List.of(randomFrom(ImpactArea.values())))
            );
        }
        HealthIndicatorResult result = service.createIndicator(
            status,
            randomAlphaOfLength(20),
            HealthIndicatorDetails.EMPTY,
            impacts,
            Collections.emptyList()
        );
        List<HealthIndicatorImpact> outputImpacts = result.impacts();
        assertEquals(3, outputImpacts.size());
        List<HealthIndicatorImpact> expectedImpacts = List.of(impact1, impact2, impact3);
        assertEquals(expectedImpacts, outputImpacts);
    }

    private HealthIndicatorService getTestHealthIndicatorService() {
        return new HealthIndicatorService() {
            @Override
            public String name() {
                return null;
            }

            @Override
            public HealthIndicatorResult calculate(boolean explain) {
                return null;
            }
        };
    }
}
