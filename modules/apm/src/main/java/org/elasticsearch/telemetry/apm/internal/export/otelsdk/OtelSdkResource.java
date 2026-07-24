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
 * {@link Resource} attached to every span and metric this node exports.
 *
 * <p>The attributes assembled here come from three layers, in order of precedence (later wins):
 * <ol>
 *   <li>The SDK defaults from {@link Resource#getDefault()}
 *   <li>Fixed Elasticsearch identity attributes set below: {@code service.name},
 *       {@code service.version}, {@code service.language.name},
 *       {@code service.runtime.name}/{@code service.runtime.version}, plus {@code service.agent.*}.</li>
 *   <li>{@code service.instance.id} from the {@code node.name} setting when configured.</li>
 *   <li>Operator-injected attributes pulled from the
 *       {@link OtelSdkSettings#TELEMETRY_RESOURCE_ATTRIBUTES} affix setting
 *       ({@code telemetry.resource.*}).
 *       This is the OTel-SDK counterpart to the {@code telemetry.agent.global_labels.*} bridge
 *       the APM-agent path uses, and it has two known producers today:
 *       <ul>
 *         <li>{@code ServerlessServerCli} maps
 *             {@code serverless.project_id}, {@code serverless.project_type} and the filtered
 *             {@code node.roles} to {@code project.id}, {@code project.type}
 *             and {@code node.tier} respectively;</li>
 *         <li>The {@code elasticsearch-controller} (serverless control plane) injects
 *             {@code orchestrator.cluster.name}, {@code channel} and
 *             {@code project.trial} into per-pod node settings.</li>
 *       </ul>
 *       The bare keys are chosen so that APM Server maps them to the same {@code labels.*} fields the
 *       APM-agent path produced. Self-managed deployments simply leave these unset.</li>
 * </ol>
 */
final class OtelSdkResource {

    private OtelSdkResource() {}

    static Resource get(Settings settings) {
        ResourceBuilder builder = Resource.builder()
            .put("service.name", "self-managed-elasticsearch") // other deployment types should override via
                                                               // telemetry.resource.service.name
            .put("service.type", "elasticsearch")
            .put("service.version", Build.current().version())
            .put("service.language.name", "java")
            .put("process.runtime.name", "Java")
            .put("process.runtime.version", Runtime.version().toString())
            .put("service.agent.name", "elasticsearch-otel-sdk")
            .put("service.agent.version", Build.current().version())
            .put("telemetry.distro.name", "elasticsearch-otel-sdk")
            .put("telemetry.distro.version", Build.current().version());
        String nodeName = settings.get("node.name");
        if (nodeName != null) {
            builder.put("service.instance.id", nodeName);
        }
        OtelSdkSettings.TELEMETRY_RESOURCE_ATTRIBUTES.getAsMap(settings).forEach(builder::put);
        return Resource.getDefault().merge(builder.build());
    }
}
