/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Map;

import static org.elasticsearch.inference.telemetry.InferenceStats.ES_PLUGIN_NAME_VALUE;

/**
 * A snapshot of node-level attributes included as constants on every inference metric.
 * These are node-scoped rather than cluster-scoped: during a rolling upgrade, different
 * nodes may report different stack versions.
 */
public record NodeTelemetryAttributes(String stackVersion, boolean isProductionRelease, String deploymentType) {

    /**
     * Builds a {@link NodeTelemetryAttributes} from the running node's environment.
     */
    public static NodeTelemetryAttributes from(Build build, Settings settings) {
        return new NodeTelemetryAttributes(
            build.version(),
            build.isProductionRelease(),
            DiscoveryNode.isStateless(settings) ? MetricAttributes.SERVERLESS_DEPLOYMENT_TYPE : MetricAttributes.HOSTED_DEPLOYMENT_TYPE
        );
    }

    /**
     * Returns the attributes as a map suitable for passing to {@link InferenceStats}.
     */
    public Map<String, Object> asMap() {
        return Map.of(
            MetricAttributes.ES_STACK_VERSION,
            stackVersion,
            MetricAttributes.ES_PRODUCTION_RELEASE,
            isProductionRelease,
            MetricAttributes.ES_DEPLOYMENT_TYPE,
            deploymentType,
            MetricAttributes.ES_PLUGIN_NAME,
            ES_PLUGIN_NAME_VALUE
        );
    }
}
