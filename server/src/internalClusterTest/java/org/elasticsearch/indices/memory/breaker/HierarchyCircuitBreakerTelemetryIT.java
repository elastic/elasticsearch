/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.memory.breaker;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE;
import static org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_TYPE_ATTRIBUTE;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = true)
public class HierarchyCircuitBreakerTelemetryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class);
    }

    public void testCircuitBreakerTripCountMetric() {
        final Settings circuitBreakerSettings = Settings.builder()
            .put(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), 100, ByteSizeUnit.BYTES)
            .put(FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), 100, ByteSizeUnit.BYTES)
            .put(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), 100, ByteSizeUnit.BYTES)
            .put(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), 150, ByteSizeUnit.BYTES)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        String dataNodeName = null;
        String masterNodeName = null;
        try {
            // NOTE: we start with empty circuitBreakerSettings to allow cluster formation
            masterNodeName = internalCluster().startMasterOnlyNode(Settings.EMPTY);
            dataNodeName = internalCluster().startDataOnlyNode(Settings.EMPTY);
            assertTrue(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(circuitBreakerSettings)
                    .get()
                    .isAcknowledged()
            );
            assertTrue(
                client().admin()
                    .indices()
                    .prepareCreate("test")
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .get()
                    .isAcknowledged()
            );
            assertEquals(
                RestStatus.OK.getStatus(),
                client().prepareIndex("test").setWaitForActiveShards(1).setSource("field", "value").get().status().getStatus()
            );
        } catch (CircuitBreakingException cbex) {
            final List<Measurement> dataNodeMeasurements = getMeasurements(dataNodeName);
            final List<Measurement> masterNodeMeasurements = getMeasurements(masterNodeName);
            final List<Measurement> allMeasurements = Stream.concat(dataNodeMeasurements.stream(), masterNodeMeasurements.stream())
                .toList();
            assertThat(allMeasurements, Matchers.not(Matchers.empty()));
            final Measurement measurement = allMeasurements.get(0);
            // Indexing fans out into internal transport requests, each of which reserves its serialized size against the
            // 100-byte inflight_requests breaker on the receiving node. A single trip is typically indices:admin/create or
            // the dynamic-mapping update indices:admin/mapping/auto_put. Two trips occur when the shard bulk
            // indices:data/write/bulk[s] trips on the data node: its transport failure makes the coordinator send a
            // internal:admin/tasks/cancel_child request back to cancel the orphaned child task, and that request trips the
            // breaker again. Which path runs depends on which node coordinates, so the count is non-deterministic (1 or 2)
            // and we only assert the breaker tripped at least once.
            assertThat(measurement.getLong(), Matchers.greaterThanOrEqualTo(1L));
            assertThat(measurement.value().longValue(), Matchers.greaterThanOrEqualTo(1L));
            assertThat(measurement.attributes(), Matchers.equalTo(Map.of(CIRCUIT_BREAKER_TYPE_ATTRIBUTE, "inflight_requests")));
            assertThat(measurement.isLong(), Matchers.equalTo(true));
            return;
        }
        fail("Expected exception not thrown");
    }

    public void testCircuitBreakerMemoryGauges() {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final String dataNodeName = internalCluster().startDataOnlyNode(Settings.EMPTY);

        final TestTelemetryPlugin plugin = telemetryPlugin(dataNodeName);
        plugin.collect();

        final List<Measurement> limits = plugin.getLongGaugeMeasurement(CircuitBreakerMetrics.ES_BREAKER_MEMORY_LIMIT);
        final List<Measurement> estimates = plugin.getLongGaugeMeasurement(CircuitBreakerMetrics.ES_BREAKER_MEMORY_ESTIMATED);

        final Set<String> expectedTypes = Set.of(
            CircuitBreaker.PARENT,
            CircuitBreaker.FIELDDATA,
            CircuitBreaker.REQUEST,
            CircuitBreaker.IN_FLIGHT_REQUESTS
        );
        assertThat(typesIn(limits), Matchers.hasItems(expectedTypes.toArray(new String[0])));
        assertThat(typesIn(estimates), Matchers.hasItems(expectedTypes.toArray(new String[0])));

        // limits and estimates are non-negative byte values; -1 means "no limit" which is also acceptable
        for (Measurement m : limits) {
            assertTrue("expected non-negative limit (or -1 sentinel) for " + m.attributes(), m.getLong() >= -1);
        }
        for (Measurement m : estimates) {
            assertTrue("expected non-negative estimate for " + m.attributes(), m.getLong() >= 0);
        }
    }

    private static Set<String> typesIn(List<Measurement> measurements) {
        return measurements.stream().map(m -> (String) m.attributes().get(BREAKER_METRIC_TYPE_ATTRIBUTE)).collect(Collectors.toSet());
    }

    private static TestTelemetryPlugin telemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    @After
    public void resetClusterSetting() {
        final var circuitBreakerSettings = Settings.builder()
            .putNull(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            .putNull(FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey())
            .putNull(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            .putNull(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey())
            .putNull(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            .putNull(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey())
            .putNull(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            .putNull(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey());
        updateClusterSettings(circuitBreakerSettings);
    }

    private List<Measurement> getMeasurements(String nodeName) {
        final TestTelemetryPlugin telemetryPlugin = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .toList()
            .get(0);
        return Measurement.combine(
            Stream.of(telemetryPlugin.getLongCounterMeasurement(CircuitBreakerMetrics.ES_BREAKER_TRIP_COUNT_TOTAL).stream())
                .flatMap(Function.identity())
                .toList()
        );
    }
}
