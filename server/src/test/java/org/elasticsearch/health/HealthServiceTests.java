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
import java.util.stream.Stream;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.UNKNOWN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class HealthServiceTests extends ESTestCase {

    public void testShouldReturnGroupedIndicators() {

        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            )
        );

        assertThat(service.getHealth(null, false), hasItems(slowTasks, networkLatency, shardsAvailable));

        assertThat(service.getHealth(null, false), hasItems(slowTasks, networkLatency, networkLatency, slowTasks));

        assertThat(service.getHealth("slow_task_assignment", false), hasItems(slowTasks));
    }

    public void testDuplicateIndicatorNames() {
        // Same indicator name, should throw exception:
        var networkLatency = new HealthIndicatorResult(
            "network_latency",
            GREEN,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        var slowTasks = new HealthIndicatorResult("network_latency", YELLOW, null, null, Collections.emptyList(), Collections.emptyList());
        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(networkLatency)
            )
        );
        expectThrows(AssertionError.class, () -> service.getHealth(null, true));
    }

    public void testMissingIndicator() {
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        var service = new HealthService(
            Collections.emptyList(),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            )
        );

        expectThrows(ResourceNotFoundException.class, "Did not find indicator indicator99", () -> service.getHealth("indicator99", false));
    }

    public void testPreflightIndicatorResultsPresent() {
        // Preflight check
        var hasMaster = new HealthIndicatorResult("has_master", GREEN, null, null, null, null);
        // Other indicators
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(hasMaster)),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            )
        );

        // Get all indicators returns preflight result mixed in with the other indicators
        List<HealthIndicatorResult> health = service.getHealth(null, false);
        assertThat(health.size(), is(equalTo(4)));
        assertThat(health, containsInAnyOrder(hasMaster, networkLatency, slowTasks, shardsAvailable));

        // Getting single indicator returns correct indicator still
        health = service.getHealth("slow_task_assignment", false);
        assertThat(health.size(), is(equalTo(1)));
        assertThat(health, contains(slowTasks));

        // Getting single preflight indicator returns preflight indicator correctly
        health = service.getHealth("has_master", false);
        assertThat(health.size(), is(equalTo(1)));
        assertThat(health, contains(hasMaster));
    }

    private void assertIndicatorIsUnknownStatus(HealthIndicatorResult result) {
        assertThat(result.status(), is(equalTo(UNKNOWN)));
        assertThat(result.symptom(), is(HealthService.UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED));
    }

    public void testPreflightIndicatorFailureTriggersUnknownResults() {
        // Preflight checks
        var hasMaster = new HealthIndicatorResult("has_master", RED, null, null, null, null);
        var hasStorage = new HealthIndicatorResult("has_storage", GREEN, null, null, null, null);
        // Other indicators
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        var service = new HealthService(
            List.of(createMockHealthIndicatorService(hasMaster), createMockHealthIndicatorService(hasStorage)),
            List.of(
                createMockHealthIndicatorService(networkLatency),
                createMockHealthIndicatorService(slowTasks),
                createMockHealthIndicatorService(shardsAvailable)
            )
        );

        {
            List<HealthIndicatorResult> health = service.getHealth(null, false);
            assertThat(health.size(), is(equalTo(5)));
            // Preflight indicators unchanged; posflight all say
            List<String> nonPreflightNames = Stream.of(networkLatency, slowTasks, shardsAvailable)
                .map(HealthIndicatorResult::name)
                .toList();
            health.stream()
                .filter(healthIndicatorResult -> nonPreflightNames.contains(healthIndicatorResult.name()))
                .forEach(this::assertIndicatorIsUnknownStatus);
            List<HealthIndicatorResult> preflightResults = List.of(hasMaster, hasStorage);
            preflightResults.forEach(healthIndicatorResult -> assertTrue(health.contains(healthIndicatorResult)));
        }

        {
            List<HealthIndicatorResult> health = service.getHealth("slow_task_assignment", false);
            assertThat(health.size(), is(equalTo(1)));
            assertIndicatorIsUnknownStatus(health.get(0));
        }

        {
            List<HealthIndicatorResult> health = service.getHealth("has_master", false);
            assertThat(health.size(), is(equalTo(1)));
            assertThat(health.get(0), is(equalTo(hasMaster)));
        }
    }

    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result) {
        return new HealthIndicatorService() {
            @Override
            public String name() {
                return result.name();
            }

            @Override
            public HealthIndicatorResult calculate(boolean explain) {
                return result;
            }
        };
    }

}
