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

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = true)
public class HierarchyCircuitBreakerTelemetryTests extends ESIntegTestCase {

    public static final String PARENT_TOTAL_CIRCUIT_BREAKER_NAME = "org.elasticsearch.indices.breaker.parent_total";
    public static final String FIELDDATA_TOTAL_CIRCUIT_BREAKER_NAME = "org.elasticsearch.indices.breaker.fielddata_total";
    public static final String REQUEST_TOTAL_CIRCUIT_BREAKER_NAME = "org.elasticsearch.indices.breaker.request_total";
    public static final String INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME = "org.elasticsearch.indices.breaker.inflight_requests_total";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestCircuitBreakerTelemetryPlugin.class);
    }

    public static class TestCircuitBreakerTelemetryPlugin extends TestTelemetryPlugin {
        protected final MeterRegistry meter = new RecordingMeterRegistry() {
            private final LongCounter inFlightRequests = new RecordingInstruments.RecordingLongCounter(
                INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME,
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
                return inFlightRequests;
            }

            @Override
            public LongCounter registerLongCounter(String name, String description, String unit) {
                assertThat(name, equalTo(INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME));
                return super.registerLongCounter(name, description, unit);
            }

            @Override
            public LongCounter getLongCounter(String name) {
                assertThat(name, equalTo(INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME));
                return super.getLongCounter(name);
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
            final TestTelemetryPlugin dataNodeTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
                .filterPlugins(TestCircuitBreakerTelemetryPlugin.class)
                .get(0);
            final List<Measurement> dataNodeMeasurements = Measurement.combine(
                Stream.of(
                    dataNodeTelemetryPlugin.getLongCounterMeasurement(INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    dataNodeTelemetryPlugin.getLongCounterMeasurement(FIELDDATA_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    dataNodeTelemetryPlugin.getLongCounterMeasurement(REQUEST_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    dataNodeTelemetryPlugin.getLongCounterMeasurement(PARENT_TOTAL_CIRCUIT_BREAKER_NAME).stream()
                ).flatMap(Function.identity()).toList()
            );
            final TestTelemetryPlugin masterNodeTelemetryPlugin = internalCluster().getInstance(PluginsService.class, masterNodeName)
                .filterPlugins(TestCircuitBreakerTelemetryPlugin.class)
                .get(0);
            final List<Measurement> masterNodeMeasurements = Measurement.combine(
                Stream.of(
                    masterNodeTelemetryPlugin.getLongCounterMeasurement(INFLIGHT_REQUESTS_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    masterNodeTelemetryPlugin.getLongCounterMeasurement(FIELDDATA_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    masterNodeTelemetryPlugin.getLongCounterMeasurement(REQUEST_TOTAL_CIRCUIT_BREAKER_NAME).stream(),
                    masterNodeTelemetryPlugin.getLongCounterMeasurement(PARENT_TOTAL_CIRCUIT_BREAKER_NAME).stream()
                ).flatMap(Function.identity()).toList()
            );
            final List<Measurement> measurements = Stream.concat(dataNodeMeasurements.stream(), masterNodeMeasurements.stream()).toList();
            assertThat(measurements, Matchers.hasSize(1));
            final Measurement measurement = measurements.get(0);
            assertThat(1L, Matchers.equalTo(measurement.getLong()));
            assertThat(1L, Matchers.equalTo(measurement.value()));
            assertThat(true, Matchers.equalTo(measurement.isLong()));
            return;
        }
        fail("Expected exception not thrown");
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
