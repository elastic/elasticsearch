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
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthServiceTests extends ESTestCase {

    public void testShouldReturnGroupedIndicators() {

        var networkLatency = new HealthIndicatorResult("network_latency", "coordination", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", "coordination", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", "data", GREEN, null, null, null, null);

        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            ),
            mockEmptyClusterService()
        );

        assertThat(
            service.getHealth(null, null, false),
            anyOf(
                hasItems(
                    new HealthComponentResult("coordination", YELLOW, List.of(slowTasks, networkLatency)),
                    new HealthComponentResult("data", GREEN, List.of(shardsAvailable))
                ),
                hasItems(
                    new HealthComponentResult("coordination", YELLOW, List.of(networkLatency, slowTasks)),
                    new HealthComponentResult("data", GREEN, List.of(shardsAvailable))
                )
            )
        );

        assertThat(
            service.getHealth("coordination", null, false),
            anyOf(
                hasItems(new HealthComponentResult("coordination", YELLOW, List.of(slowTasks, networkLatency))),
                hasItems(new HealthComponentResult("coordination", YELLOW, List.of(networkLatency, slowTasks)))
            )
        );

        assertThat(
            service.getHealth("coordination", "slow_task_assignment", false),
            hasItems(new HealthComponentResult("coordination", null, List.of(slowTasks)))
        );
    }

    public void testDuplicateIndicatorNames() {
        // Same component, same indicator name, should throw exception:
        var networkLatency = new HealthIndicatorResult(
            "network_latency",
            "coordination",
            GREEN,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        var slowTasks = new HealthIndicatorResult(
            "network_latency",
            "coordination",
            YELLOW,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        expectThrows(AssertionError.class, () -> HealthService.createComponentFromIndicators(List.of(networkLatency, slowTasks), true));
    }

    public void testMissingComponentOrIndicator() {
        var networkLatency = new HealthIndicatorResult("network_latency", "coordination", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", "coordination", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", "data", GREEN, null, null, null, null);

        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
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
            () -> service.getHealth("coordination", "indicator99", false)
        );
    }

    public void testPreflightIndicatorResultsPresent() {
        // Preflight check
        var hasMaster = new HealthIndicatorResult("has_master", "coordination", GREEN, null, null, null, null);
        // Other indicators
        var networkLatency = new HealthIndicatorResult("network_latency", "coordination", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", "coordination", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", "data", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(hasMaster)),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            ),
            mockEmptyClusterService()
        );

        // Get all indicators returns preflight result mixed in with appropriate component
        List<HealthComponentResult> health = service.getHealth(null, null, false);
        assertThat(health.size(), is(equalTo(2)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.status(), is(equalTo(YELLOW)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), containsInAnyOrder(hasMaster, networkLatency, slowTasks));
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("data")).findAny().orElseThrow();
            assertThat(component2.status(), is(equalTo(GREEN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators(), contains(shardsAvailable));
        }

        // Getting single component returns preflight result mixed in with appropriate component
        health = service.getHealth("coordination", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.status(), is(equalTo(YELLOW)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), containsInAnyOrder(hasMaster, networkLatency, slowTasks));
        }

        // Getting single indicator returns correct indicator still
        health = service.getHealth("coordination", "slow_task_assignment", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), contains(slowTasks));
        }

        // Getting single preflight indicator returns preflight indicator correctly
        health = service.getHealth("coordination", "has_master", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators(), contains(hasMaster));
        }
    }

    private void assertIndicatorIsUnknownStatus(HealthIndicatorResult result) {
        assertThat(result.status(), is(equalTo(UNKNOWN)));
        assertThat(
            result.summary(),
            is(oneOf(HealthService.UNKNOWN_RESULT_SUMMARY_NOT_RECOVERED, HealthService.UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED))
        );
    }

    public void testPreflightIndicatorFailureTriggersUnknownResults() {
        // Preflight checks
        var hasMaster = new HealthIndicatorResult("has_master", "coordination", RED, null, null, null, null);
        var hasStorage = new HealthIndicatorResult("has_storage", "data", GREEN, null, null, null, null);
        // Other indicators
        var networkLatency = new HealthIndicatorResult("network_latency", "coordination", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", "coordination", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", "data", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(hasMaster), createMockHealthIndicatorService(hasStorage)),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            ),
            mockEmptyClusterService()
        );

        List<HealthComponentResult> health = service.getHealth(null, null, false);
        assertThat(health.size(), is(equalTo(2)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            // RED because hasMaster was RED
            assertThat(component1.status(), is(equalTo(RED)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertThat(hasMasterResult, is(equalTo(hasMaster)));
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult networkLatencyResult = component1.findIndicator("network_latency");
            assertIndicatorIsUnknownStatus(networkLatencyResult);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("data")).findAny().orElseThrow();
            // UNKNOWN because shardsAvailable will be marked UNKNOWN because hasMaster is RED
            assertThat(component2.status(), is(equalTo(UNKNOWN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators().size(), is(equalTo(2)));
            // Preflight 2 should be returned as is
            HealthIndicatorResult hasStorageResult = component2.findIndicator("has_storage");
            assertThat(hasStorageResult, is(equalTo(hasStorage)));
            // Indicator 3 should be UNKNOWN
            HealthIndicatorResult shardsAvailableResult = component2.findIndicator("shards_availability");
            assertIndicatorIsUnknownStatus(shardsAvailableResult);
        }

        health = service.getHealth("coordination", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            // RED because hasMaster was RED
            assertThat(component1.status(), is(equalTo(RED)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertThat(hasMasterResult, is(equalTo(hasMaster)));
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult networkLatencyResult = component1.findIndicator("network_latency");
            assertIndicatorIsUnknownStatus(networkLatencyResult);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }

        health = service.getHealth("coordination", "slow_task_assignment", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }

        health = service.getHealth("coordination", "has_master", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Preflight 1 should be returned as is
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertThat(hasMasterResult, is(equalTo(hasMaster)));
        }
    }

    public void testAllIndicatorsUnknownWhenClusterStateNotRecovered() {
        var hasMaster = new HealthIndicatorResult("has_master", "coordination", RED, null, null, null, null);
        var hasStorage = new HealthIndicatorResult("has_storage", "data", GREEN, null, null, null, null);
        var networkLatency = new HealthIndicatorResult("network_latency", "coordination", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", "coordination", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", "data", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(hasMaster), createMockHealthIndicatorService(hasStorage)),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
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
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component1.status(), is(equalTo(UNKNOWN)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertIndicatorIsUnknownStatus(hasMasterResult);
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult networkLatencyResult = component1.findIndicator("network_latency");
            assertIndicatorIsUnknownStatus(networkLatencyResult);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }
        {
            HealthComponentResult component2 = health.stream().filter(result -> result.name().equals("data")).findAny().orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component2.status(), is(equalTo(UNKNOWN)));
            assertThat(component2.indicators(), is(notNullValue()));
            assertThat(component2.indicators().size(), is(equalTo(2)));
            // Preflight 2 should be UNKNOWN
            HealthIndicatorResult hasStorageResult = component2.findIndicator("has_storage");
            assertIndicatorIsUnknownStatus(hasStorageResult);
            // Indicator 3 should be UNKNOWN
            HealthIndicatorResult shardsAvailableResult = component2.findIndicator("shards_availability");
            assertIndicatorIsUnknownStatus(shardsAvailableResult);
        }

        health = service.getHealth("coordination", null, false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            // UNKNOWN because cluster state not recovered
            assertThat(component1.status(), is(equalTo(UNKNOWN)));
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(3)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertIndicatorIsUnknownStatus(hasMasterResult);
            // Indicator 1 should be UNKNOWN
            HealthIndicatorResult networkLatencyResult = component1.findIndicator("network_latency");
            assertIndicatorIsUnknownStatus(networkLatencyResult);
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }

        health = service.getHealth("coordination", "slow_task_assignment", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Indicator 2 should be UNKNOWN
            HealthIndicatorResult slowTasksResult = component1.findIndicator("slow_task_assignment");
            assertIndicatorIsUnknownStatus(slowTasksResult);
        }

        health = service.getHealth("coordination", "has_master", false);
        assertThat(health.size(), is(equalTo(1)));
        {
            HealthComponentResult component1 = health.stream()
                .filter(result -> result.name().equals("coordination"))
                .findAny()
                .orElseThrow();
            assertThat(component1.indicators(), is(notNullValue()));
            assertThat(component1.indicators().size(), is(equalTo(1)));
            // Preflight 1 should be UNKNOWN
            HealthIndicatorResult hasMasterResult = component1.findIndicator("has_master");
            assertIndicatorIsUnknownStatus(hasMasterResult);
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
            public HealthIndicatorResult calculate(boolean explain) {
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
