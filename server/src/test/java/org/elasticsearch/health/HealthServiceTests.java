/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.UNKNOWN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthServiceTests extends ESTestCase {

    public void testShouldReturnGroupedIndicators() {

        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            ),
            mockEmptyClusterService()
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

    public void testDuplicateIndicatorNames() {
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
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            ),
            mockEmptyClusterService()
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

    public void testPreflightIndicatorResultsPresent() {
        var preflight1 = new HealthIndicatorResult("preflight1", "component1", GREEN, null, null, null, null);
        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(preflight1)),
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            ),
            mockEmptyClusterService()
        );

        // Get all indicators returns preflight result mixed in with appropriate component
        List<HealthComponentResult> health = service.getHealth(null, null, false);
        assertThat(health.size(), is(equalTo(2)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.status(), is(equalTo(YELLOW)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), containsInAnyOrder(preflight1, indicator1, indicator2));
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("component2")).findAny().orElseThrow();
            assertThat(component2.status(), is(equalTo(GREEN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators(), contains(indicator3));
        }

        // Getting single component returns preflight result mixed in with appropriate component
        health = service.getHealth("component1", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.status(), is(equalTo(YELLOW)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), containsInAnyOrder(preflight1, indicator1, indicator2));
        }

        // Getting single indicator returns correct indicator still
        health = service.getHealth("component1", "indicator2", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), contains(indicator2));
        }

        // Getting single preflight indicator returns preflight indicator correctly
        health = service.getHealth("component1", "preflight1", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), contains(preflight1));
        }
    }

    private void assertIndicatorIsUnknownStatus(HealthIndicatorResult result) {
        assertThat(result.status(), is(equalTo(UNKNOWN)));
        assertThat(result.summary(), is(equalTo(HealthService.UNKNOWN_RESULT_SUMMARY)));
    }

    public void testPreflightIndicatorFailureTriggersUnknownResults() {
        var preflight1 = new HealthIndicatorResult("preflight1", "component1", RED, null, null, null, null);
        var preflight2 = new HealthIndicatorResult("preflight2", "component2", GREEN, null, null, null, null);
        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(preflight1), createMockHealthIndicatorService(preflight2)),
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            ),
            mockEmptyClusterService()
        );

        List<HealthComponentResult> health = service.getHealth(null, null, false);
        assertThat(health.size(), is(equalTo(2)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            // RED because preflight1 was RED
            assertThat(component1.status(), is(equalTo(RED)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertThat(preflight1Result, is(equalTo(preflight1)));
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult indicator1Result = component1.findIndicator("indicator1");
            assertIndicatorIsUnknownStatus(indicator1Result);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("component2")).findAny().orElseThrow();
            // UNKNOWN because indicator3 will be marked UNKNOWN because preflight1 is RED
            assertThat(component2.status(), is(equalTo(UNKNOWN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators().size(), is(equalTo(2)));
            // Preflight 2 should be returned as is
            HealthIndicatorResult preflight2Result = component2.findIndicator("preflight2");
            assertThat(preflight2Result, is(equalTo(preflight2)));
            // Indicator 3 should be UNKNOWN
            HealthIndicatorResult indicator3Result = component2.findIndicator("indicator3");
            assertIndicatorIsUnknownStatus(indicator3Result);
        }

        health = service.getHealth("component1", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            // RED because preflight1 was RED
            assertThat(component1.status(), is(equalTo(RED)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertThat(preflight1Result, is(equalTo(preflight1)));
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult indicator1Result = component1.findIndicator("indicator1");
            assertIndicatorIsUnknownStatus(indicator1Result);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }

        health = service.getHealth("component1", "indicator2", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }

        health = service.getHealth("component1", "preflight1", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertThat(preflight1Result, is(equalTo(preflight1)));
        }
    }

    public void testAllIndicatorsUnknownWhenClusterStateNotRecovered() {
        var preflight1 = new HealthIndicatorResult("preflight1", "component1", RED, null, null, null, null);
        var preflight2 = new HealthIndicatorResult("preflight2", "component2", GREEN, null, null, null, null);
        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(preflight1), createMockHealthIndicatorService(preflight2)),
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            ),
            mockClusterService(
                ClusterState.builder(new ClusterName("test-cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                    .build()
            )
        );

        List<HealthComponentResult> health = service.getHealth(null, null, false);
        assertThat(health.size(), is(equalTo(2)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component1.status(), is(equalTo(UNKNOWN)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertIndicatorIsUnknownStatus(preflight1Result);
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult indicator1Result = component1.findIndicator("indicator1");
            assertIndicatorIsUnknownStatus(indicator1Result);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("component2")).findAny().orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component2.status(), is(equalTo(UNKNOWN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators().size(), is(equalTo(2)));
            // Preflight 2 should be UNKNOWN
            HealthIndicatorResult preflight2Result = component2.findIndicator("preflight2");
            assertIndicatorIsUnknownStatus(preflight2Result);
            // Indicator 3 should be UNKNOWN
            HealthIndicatorResult indicator3Result = component2.findIndicator("indicator3");
            assertIndicatorIsUnknownStatus(indicator3Result);
        }

        health = service.getHealth("component1", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component1.status(), is(equalTo(UNKNOWN)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertIndicatorIsUnknownStatus(preflight1Result);
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult indicator1Result = component1.findIndicator("indicator1");
            assertIndicatorIsUnknownStatus(indicator1Result);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }

        health = service.getHealth("component1", "indicator2", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult indicator2Result = component1.findIndicator("indicator2");
            assertIndicatorIsUnknownStatus(indicator2Result);
        }

        health = service.getHealth("component1", "preflight1", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream().filter(result -> result.name().equals("component1")).findAny().orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult preflight1Result = component1.findIndicator("preflight1");
            assertIndicatorIsUnknownStatus(preflight1Result);
        }
    }

    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result) {
        return new HealthIndicatorService() {
            @Override
            public String name() {
                return result.name();
            }

            @Override
            public String component() {
                return result.component();
            }

            @Override
            public HealthIndicatorResult calculate(boolean calculateDetails) {
                return result;
            }
        };
    }

    private static ClusterService mockEmptyClusterService() {
        return mockClusterService(ClusterState.EMPTY_STATE);
    }

    private static ClusterService mockClusterService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }
}
