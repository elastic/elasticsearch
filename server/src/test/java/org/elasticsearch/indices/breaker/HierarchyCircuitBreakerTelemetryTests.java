/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingInstruments;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_TYPE_ATTRIBUTE;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = true)
public class HierarchyCircuitBreakerTelemetryTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestCircuitBreakerTelemetryPlugin.class);
    }

    public static class TestCircuitBreakerTelemetryPlugin extends TestTelemetryPlugin {
        protected final MeterRegistry meter = new RecordingMeterRegistry() {
            private final LongCounter tripCount = new RecordingInstruments.RecordingLongCounter(
                CircuitBreakerMetrics.ES_BREAKER_TRIP_COUNT_TOTAL,
                recorder
            ) {
                @Override
                public void incrementBy(long inc) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void incrementBy(long inc, Map<String, Object> attributes) {
                    throw new UnsupportedOperationException();
                }
            };

            @Override
            protected LongCounter buildLongCounter(String name, String description, String unit) {
                if (name.equals(tripCount.getName())) {
                    return tripCount;
                }
                throw new IllegalArgumentException("Unknown counter metric name [" + name + "]");
            }

            @Override
            public LongCounter registerLongCounter(String name, String description, String unit) {
                assertCircuitBreakerName(name);
                return super.registerLongCounter(name, description, unit);
            }

            @Override
            public LongCounter getLongCounter(String name) {
                assertCircuitBreakerName(name);
                return super.getLongCounter(name);
            }

            private void assertCircuitBreakerName(final String name) {
                assertThat(name, Matchers.oneOf(CircuitBreakerMetrics.ES_BREAKER_TRIP_COUNT_TOTAL));
            }
        };
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
            assertTrue(clusterAdmin().prepareUpdateSettings().setPersistentSettings(circuitBreakerSettings).get().isAcknowledged());
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
            assertThat(1L, Matchers.equalTo(measurement.getLong()));
            assertThat(1L, Matchers.equalTo(measurement.value()));
            assertThat(Map.of(CIRCUIT_BREAKER_TYPE_ATTRIBUTE, "inflight_requests"), Matchers.equalTo(measurement.attributes()));
            assertThat(true, Matchers.equalTo(measurement.isLong()));
            return;
        }
        fail("Expected exception not thrown");
    }

    private List<Measurement> getMeasurements(String dataNodeName) {
        final TestTelemetryPlugin dataNodeTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestCircuitBreakerTelemetryPlugin.class)
            .toList()
            .get(0);
        return Measurement.combine(
            Stream.of(dataNodeTelemetryPlugin.getLongCounterMeasurement(CircuitBreakerMetrics.ES_BREAKER_TRIP_COUNT_TOTAL).stream())
                .flatMap(Function.identity())
                .toList()
        );
    }

    // Make sure circuit breaker telemetry on trip count reports the same values as circuit breaker stats
    private void assertCircuitBreakerTripCount(
        final HierarchyCircuitBreakerService circuitBreakerService,
        final String circuitBreakerName,
        int firstBytesEstimate,
        int secondBytesEstimate,
        long expectedTripCountValue
    ) {
        try {
            circuitBreakerService.getBreaker(circuitBreakerName).addEstimateBytesAndMaybeBreak(firstBytesEstimate, randomAlphaOfLength(5));
            circuitBreakerService.getBreaker(circuitBreakerName).addEstimateBytesAndMaybeBreak(secondBytesEstimate, randomAlphaOfLength(5));
        } catch (final CircuitBreakingException cbex) {
            final CircuitBreakerStats circuitBreakerStats = Arrays.stream(circuitBreakerService.stats().getAllStats())
                .filter(stats -> circuitBreakerName.equals(stats.getName()))
                .findAny()
                .get();
            assertThat(circuitBreakerService.getBreaker(circuitBreakerName).getTrippedCount(), Matchers.equalTo(expectedTripCountValue));
            assertThat(circuitBreakerStats.getTrippedCount(), Matchers.equalTo(expectedTripCountValue));
        }
    }

}
