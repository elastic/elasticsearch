/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.telemetry.TransformCrossProjectMetrics;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

/**
 * Verifies that the per-node CPS gauges are actually published through the telemetry
 * infrastructure by a running node: the component is constructed (CPS enabled + transform
 * role), the gauges are registered, and a running transform's facts flow through the
 * {@code TransformNode} task registry, the background poll, and the gauge observers.
 * <p>
 * Only the {@code legacy} / {@code origin} attribute values are exercisable here: the
 * {@code uiam} side requires a real UIAM service to mint credentials and the
 * {@code cross_project} side requires linked projects — both exist only in the serverless
 * two-cluster setup.
 */
public class TransformCrossProjectMetricsIT extends TransformSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(TransformCrossProjectIT.CpsPlugin.class, TestTelemetryPlugin.class))
            .toList();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .put(TransformCrossProjectMetrics.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(10))
            .build();
    }

    public void testCpsGaugesPublishForRunningTransform() throws Exception {
        var telemetry = getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();

        // both gauges are registered at node startup (CPS enabled + transform role)
        assertThat(
            telemetry.getRegisteredMetrics(InstrumentType.LONG_GAUGE),
            hasItems(
                TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT,
                TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT
            )
        );

        var transformSrc = "reviews_cps_metrics";
        createSourceIndex(transformSrc);
        indexRandomDiceDoc(transformSrc);

        var transformId = "transform_cps_metrics";
        createDiceTransform(transformId, transformSrc, IndicesOptions.CPS_LENIENT_EXPAND_OPEN, null);
        startTransform(transformId);

        try {
            // once the first search completes and the 10s poll fires, the node publishes:
            // no UIAM service locally -> legacy; no linked projects -> origin
            assertBusy(() -> {
                telemetry.resetMeter();
                telemetry.collect();
                assertThat(
                    measurement(telemetry, TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT, "auth_type", "legacy"),
                    equalTo(1L)
                );
                assertThat(
                    measurement(telemetry, TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT, "auth_type", "uiam"),
                    equalTo(0L)
                );
                assertThat(
                    measurement(telemetry, TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT, "scope", "origin"),
                    equalTo(1L)
                );
                assertThat(
                    measurement(telemetry, TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT, "scope", "cross_project"),
                    equalTo(0L)
                );
            }, 30, TimeUnit.SECONDS);
        } finally {
            stopTransform(transformId);
        }

        // stopped -> the task deregistered itself -> the next poll caches zero counts -> node emits nothing
        assertBusy(() -> {
            telemetry.resetMeter();
            telemetry.collect();
            assertThat(telemetry.getLongGaugeMeasurement(TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT), empty());
            assertThat(telemetry.getLongGaugeMeasurement(TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT), empty());
        }, 30, TimeUnit.SECONDS);

        deleteTransform(transformId);
    }

    private static long measurement(TestTelemetryPlugin telemetry, String metric, String attribute, String value) {
        var matches = telemetry.getLongGaugeMeasurement(metric)
            .stream()
            .filter(measurement -> value.equals(measurement.attributes().get(attribute)))
            .toList();
        assertThat(metric + " should emit exactly one measurement for " + attribute + "=" + value, matches, hasSize(1));
        return matches.getFirst().getLong();
    }

    private void startTransform(String transformId) {
        var request = new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS);
        assertTrue(client().execute(StartTransformAction.INSTANCE, request).actionGet(TimeValue.THIRTY_SECONDS).isAcknowledged());
    }
}
