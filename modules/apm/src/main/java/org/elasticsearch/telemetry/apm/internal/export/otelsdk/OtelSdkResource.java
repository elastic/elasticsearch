/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;

/**
 * Shared OTel {@link Resource} for the metric and trace export paths so the same node emits
 * the same identity attributes regardless of telemetry type. {@link Resource#getDefault()}
 * contributes the SDK's auto-loaded {@code telemetry.sdk.{name,version,language}}. APM-intake
 * naming ({@code service.agent.{name,version}}, {@code service.language.name}) is emitted
 * alongside the OTel SemConv equivalents ({@code telemetry.distro.{name,version}}) so consumers
 * on either scheme see equivalent values during the agent-to-SDK migration. {@code service.instance.id}
 * is set to the {@code node.name} setting when available, mirroring the agent path's
 * {@code service_node_name}.
 */
final class OtelSdkResource {

    private OtelSdkResource() {}

    static Resource get(Settings settings) {
        ResourceBuilder builder = Resource.builder()
            .put("service.name", "elasticsearch")
            .put("service.version", Build.current().version())
            .put("service.agent.name", "elasticsearch-otel-sdk")
            .put("service.agent.version", Build.current().version())
            .put("telemetry.distro.name", "elasticsearch-otel-sdk")
            .put("telemetry.distro.version", Build.current().version())
            .put("service.language.name", "java");
        String nodeName = settings.get("node.name");
        if (nodeName != null) {
            builder.put("service.instance.id", nodeName);
        }
        return Resource.getDefault().merge(builder.build());
    }
}
