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
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.inference.telemetry.InferenceStats.ES_PLUGIN_NAME_VALUE;
import static org.hamcrest.Matchers.is;

public class NodeTelemetryAttributesTests extends ESTestCase {

    public void testDeploymentType_IsServerless_WhenSettings_IsTrue() {
        var settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        var attrs = NodeTelemetryAttributes.from(Build.current(), settings);

        assertThat(attrs.deploymentType(), is(MetricAttributes.SERVERLESS_DEPLOYMENT_TYPE));
    }

    public void testDeploymentType_IsHosted_WhenSettings_IsFalse() {
        var settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, false).build();
        var attrs = NodeTelemetryAttributes.from(Build.current(), settings);

        assertThat(attrs.deploymentType(), is(MetricAttributes.HOSTED_DEPLOYMENT_TYPE));
    }

    public void testDeploymentType_IsHosted_WhenSettings_IsAbsent() {
        var attrs = NodeTelemetryAttributes.from(Build.current(), Settings.EMPTY);

        assertThat(attrs.deploymentType(), is(MetricAttributes.HOSTED_DEPLOYMENT_TYPE));
    }

    public void testAsMapContainsAllKeys() {
        var version = "9.0.0";
        var attrs = new NodeTelemetryAttributes(version, true, MetricAttributes.HOSTED_DEPLOYMENT_TYPE);

        assertThat(
            attrs.asMap(),
            is(
                Map.of(
                    MetricAttributes.ES_STACK_VERSION,
                    version,
                    MetricAttributes.ES_PRODUCTION_RELEASE,
                    true,
                    MetricAttributes.ES_DEPLOYMENT_TYPE,
                    MetricAttributes.HOSTED_DEPLOYMENT_TYPE,
                    MetricAttributes.ES_PLUGIN_NAME,
                    ES_PLUGIN_NAME_VALUE
                )
            )
        );
    }
}
