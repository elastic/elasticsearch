/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.test.ClusterServiceUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Registers the settings listeners of an {@link APMTracingService} the way the node does, in a cluster named {@value #CLUSTER_NAME} and
 * a node named {@value #NODE_NAME}, and applies changes of the tracing settings to it, like an update of the cluster settings does.
 */
public final class TracingSettingsUpdater {

    public static final String CLUSTER_NAME = "test-cluster";
    public static final String NODE_NAME = "test-node";

    private final ClusterSettings clusterSettings;
    private Settings current;

    /**
     * @param initial the settings that the service was created with
     */
    public TracingSettingsUpdater(APMTracingService service, Settings initial) {
        this.current = initial;
        // the cluster service registers its own settings too
        final Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(
            Set.of(
                APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING,
                APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING,
                APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING,
                APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES,
                OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH,
                OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS,
                OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS,
                OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE
            )
        );
        this.clusterSettings = new ClusterSettings(initial, registered);
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(
            new DeterministicTaskQueue().getThreadPool(),
            DiscoveryNodeUtils.create(NODE_NAME, NODE_NAME),
            Settings.builder().put("node.name", NODE_NAME).put("cluster.name", CLUSTER_NAME).build(),
            clusterSettings
        );
        service.addClusterSettingsListeners(clusterService);
    }

    /** Applies the changes on top of the settings that are current. */
    public void update(Settings changes) {
        current = Settings.builder().put(current).put(changes).build();
        clusterSettings.applySettings(current);
    }
}
