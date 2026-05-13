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
 * Shared OTel {@link Resource} for metric and trace export paths so the same node both APM-intake
 * ({@code service.agent.*}) and OTel SemConv ({@code telemetry.distro.*}) identity attributes
 * alongside the SDK defaults from {@link Resource#getDefault()}.
 */
final class OtelSdkResource {

    private OtelSdkResource() {}

    static Resource get(Settings settings) {
        ResourceBuilder builder = Resource.builder()
            .put("service.name", "elasticsearch")
            .put("service.version", Build.current().version())
            .put("service.language.name", "java")
            .put("service.agent.name", "elasticsearch-otel-sdk")
            .put("service.agent.version", Build.current().version())
            .put("telemetry.distro.name", "elasticsearch-otel-sdk")
            .put("telemetry.distro.version", Build.current().version());
        String nodeName = settings.get("node.name");
        if (nodeName != null) {
            builder.put("service.instance.id", nodeName);
        }
        return Resource.getDefault().merge(builder.build());
    }
}
