/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.OpenTelemetry;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class APMMeterServiceTests extends ESTestCase {

    private final AtomicInteger otelMeters = new AtomicInteger();
    private final AtomicInteger noopMeters = new AtomicInteger();

    public void testEnablingMetricsSwitchesToTheExportingMeter() {
        final Settings initial = metrics(false);
        final var service = service(initial);
        final var clusterSettings = registerListeners(service, initial);
        assertThat(otelMeters.get(), equalTo(0));

        clusterSettings.applySettings(metrics(true));

        assertThat(otelMeters.get(), equalTo(1));
    }

    public void testDisablingMetricsSwitchesToTheNoopMeter() {
        final Settings initial = metrics(true);
        final var service = service(initial);
        final var clusterSettings = registerListeners(service, initial);
        final int noopBefore = noopMeters.get();

        clusterSettings.applySettings(metrics(false));

        assertThat(noopMeters.get(), equalTo(noopBefore + 1));
    }

    /** Counts the meters that the service asks for, since that is how it switches between exporting and not exporting. */
    private APMMeterService service(Settings settings) {
        return new APMMeterService(settings, () -> {
            otelMeters.incrementAndGet();
            return OpenTelemetry.noop().getMeter("otel");
        }, () -> {
            noopMeters.incrementAndGet();
            return OpenTelemetry.noop().getMeter("noop");
        });
    }

    private static ClusterSettings registerListeners(APMMeterService service, Settings initial) {
        final Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING);
        registered.add(OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED);
        final ClusterSettings clusterSettings = new ClusterSettings(initial, registered);
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(
            new DeterministicTaskQueue().getThreadPool(),
            DiscoveryNodeUtils.create("node", "node"),
            Settings.EMPTY,
            clusterSettings
        );
        service.addClusterSettingsListeners(clusterService);
        return clusterSettings;
    }

    private static Settings metrics(boolean enabled) {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), enabled).build();
    }
}
