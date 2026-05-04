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

import org.elasticsearch.Build;

/**
 * Shared OTel {@link Resource} for the metric and trace export paths so the same node emits
 * the same identity attributes regardless of telemetry type. {@link Resource#getDefault()}
 * contributes the SDK's auto-loaded {@code telemetry.sdk.{name,version,language}}; the
 * APM-intake-style {@code service.agent.{name,version}} and {@code service.language.name}
 * are layered on top so downstream consumers that read either naming scheme see both.
 */
final class OtelSdkResource {

    private OtelSdkResource() {}

    static Resource get() {
        return Resource.getDefault()
            .merge(
                Resource.builder()
                    .put("service.name", "elasticsearch")
                    .put("service.version", Build.current().version())
                    .put("service.agent.name", "elasticsearch-otel-sdk")
                    .put("service.agent.version", Build.current().version())
                    .put("service.language.name", "java")
                    .build()
            );
    }
}
