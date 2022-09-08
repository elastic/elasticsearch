/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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

        NodeClient client = getTestClient(HealthInfo.EMPTY_HEALTH_INFO);

        assertThat(service.getHealth(client, null, false), hasItems(slowTasks, networkLatency, shardsAvailable));

        assertThat(service.getHealth(client, null, false), hasItems(slowTasks, networkLatency, networkLatency, slowTasks));

        assertThat(service.getHealth(client, "slow_task_assignment", false), hasItems(slowTasks));
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
        NodeClient client = getTestClient(HealthInfo.EMPTY_HEALTH_INFO);
        expectThrows(AssertionError.class, () -> service.getHealth(client, null, true));
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
        NodeClient client = getTestClient(HealthInfo.EMPTY_HEALTH_INFO);
        expectThrows(
            ResourceNotFoundException.class,
            "Did not find indicator indicator99",
            () -> service.getHealth(client, "indicator99", false)
        );
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
        NodeClient client = getTestClient(HealthInfo.EMPTY_HEALTH_INFO);

        // Get all indicators returns preflight result mixed in with the other indicators
        List<HealthIndicatorResult> health = service.getHealth(client, null, false);
        assertThat(health.size(), is(equalTo(4)));
        assertThat(health, containsInAnyOrder(hasMaster, networkLatency, slowTasks, shardsAvailable));

        // Getting single indicator returns correct indicator still
        health = service.getHealth(client, "slow_task_assignment", false);
        assertThat(health.size(), is(equalTo(1)));
        assertThat(health, contains(slowTasks));

        // Getting single preflight indicator returns preflight indicator correctly
        health = service.getHealth(client, "has_master", false);
        assertThat(health.size(), is(equalTo(1)));
        assertThat(health, contains(hasMaster));
    }

    public void testThatIndicatorsGetHealthInfoData() {
        /*
         * This test makes sure that HealthService is passing the data returned by the FetchHealthInfoCacheAction to all of the
         * HealthIndicatorServices except for the preflight ones.
         */
        // Preflight check
        var hasMaster = new HealthIndicatorResult("has_master", GREEN, null, null, null, null);
        // Other indicators
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);
        Map<String, DiskHealthInfo> diskHealthInfoMap = new HashMap<>();
        diskHealthInfoMap.put(
            randomAlphaOfLength(30),
            new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
        );
        HealthInfo healthInfo = new HealthInfo(diskHealthInfoMap);

        var service = new HealthService(
            // The preflight indicator does not get data because the data is not fetched until after the preflight check
            List.of(createMockHealthIndicatorService(hasMaster, HealthInfo.EMPTY_HEALTH_INFO)),
            List.of(
                createMockHealthIndicatorService(networkLatency, healthInfo),
                createMockHealthIndicatorService(slowTasks, healthInfo),
                createMockHealthIndicatorService(shardsAvailable, healthInfo)
            )
        );
        NodeClient client = getTestClient(healthInfo);

        // Get all indicators returns preflight result mixed in with the other indicators
        service.getHealth(client, null, false);
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
        NodeClient client = getTestClient(HealthInfo.EMPTY_HEALTH_INFO);
        {
            List<HealthIndicatorResult> health = service.getHealth(client, null, false);
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
            List<HealthIndicatorResult> health = service.getHealth(client, "slow_task_assignment", false);
            assertThat(health.size(), is(equalTo(1)));
            assertIndicatorIsUnknownStatus(health.get(0));
        }

        {
            List<HealthIndicatorResult> health = service.getHealth(client, "has_master", false);
            assertThat(health.size(), is(equalTo(1)));
            assertThat(health.get(0), is(equalTo(hasMaster)));
        }
    }

    /**
     * This returns a mocked NodeClient that will return the given HealthInfo if the FetchHealthInfoCacheAction is called.
     * @param healthInfo The HealthInfo that will be returned if this client calls the FetchHealthInfoCacheAction
     * @return A mocked NodeClient
     */
    @SuppressWarnings("unchecked")
    private NodeClient getTestClient(HealthInfo healthInfo) {
        NodeClient client = mock(NodeClient.class);
        doAnswer(invocation -> {
            ActionListener<FetchHealthInfoCacheAction.Response> actionListener = invocation.getArgument(2, ActionListener.class);
            actionListener.onResponse(new FetchHealthInfoCacheAction.Response(healthInfo));
            return null;
        }).when(client).doExecute(any(ActionType.class), any(), any(ActionListener.class));
        return client;
    }

    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result) {
        return createMockHealthIndicatorService(result, null);
    }

    /**
     * This returns a test HealthIndicatorService
     * @param result The HealthIndicatorResult that will be returned by the calculate method when the HealthIndicatorService returned by
     *               this method is called
     * @param expectedHealthInfo If this HealthInfo is not null then the returned HealthIndicatorService's calculate method will assert
     *                           that the HealthInfo it is passed is equal to this when it is called
     * @return A test HealthIndicatorService
     */
    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result, HealthInfo expectedHealthInfo) {
        return new HealthIndicatorService() {
            @Override
            public String name() {
                return result.name();
            }

            @Override
            public HealthIndicatorResult calculate(boolean explain, HealthInfo healthInfo) {
                if (expectedHealthInfo != null && HealthNode.isEnabled()) {
                    assertThat(healthInfo, equalTo(expectedHealthInfo));
                }
                return result;
            }
        };
    }
}
