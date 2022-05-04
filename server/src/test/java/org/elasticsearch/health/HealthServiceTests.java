/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthServiceTests extends ESTestCase {

    public void testShouldReturnGroupedIndicators() {

        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            )
        );

        assertThat(
            service.getHealth(null, null, false),
            anyOf(
                hasItems(
                    new HealthComponentResult("component1", YELLOW, List.of(indicator2, indicator1)),
                    new HealthComponentResult("component2", GREEN, List.of(indicator3))
                ),
                hasItems(
                    new HealthComponentResult("component1", YELLOW, List.of(indicator1, indicator2)),
                    new HealthComponentResult("component2", GREEN, List.of(indicator3))
                )
            )
        );

        assertThat(
            service.getHealth("component1", null, false),
            anyOf(
                hasItems(new HealthComponentResult("component1", YELLOW, List.of(indicator2, indicator1))),
                hasItems(new HealthComponentResult("component1", YELLOW, List.of(indicator1, indicator2)))
            )
        );

        assertThat(
            service.getHealth("component1", "indicator2", false),
            hasItems(new HealthComponentResult("component1", null, List.of(indicator2)))
        );
    }

    public void testDuplicateIndicatorNamess() {
        // Same component, same indicator name, should throw exception:
        var indicator1 = new HealthIndicatorResult(
            "indicator1",
            "component1",
            GREEN,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        var indicator2 = new HealthIndicatorResult(
            "indicator1",
            "component1",
            YELLOW,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        expectThrows(AssertionError.class, () -> HealthService.createComponentFromIndicators(List.of(indicator1, indicator2), true));
    }

    public void testMissingComponentOrIndicator() {
        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            )
        );

        expectThrows(
            ResourceNotFoundException.class,
            "Did not find component component99",
            () -> service.getHealth("component99", null, false)
        );

        expectThrows(
            ResourceNotFoundException.class,
            "Did not find indicator indicator99 in component component1",
            () -> service.getHealth("component1", "indicator99", false)
        );
    }

    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result) {
        var healthIndicatorService = mock(HealthIndicatorService.class);
        when(healthIndicatorService.calculate(false)).thenReturn(result);
        when(healthIndicatorService.component()).thenReturn(result.component());
        when(healthIndicatorService.name()).thenReturn(result.name());
        return healthIndicatorService;
    }
}
