/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class OtelSdkResourceTests extends ESTestCase {

    public void testOtelSdkResourceBuilding() {
        Settings settings = Settings.builder()
            .put("node.name", "node-7")
            .put("telemetry.resource.project.id", "abc-123")
            .put("telemetry.resource.orchestrator.cluster.name", "es-prod-eu-west-1")
            .build();

        Resource resource = OtelSdkResource.get(settings);

        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), is("self-managed-elasticsearch"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.type")), is("elasticsearch"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.version")), is(Build.current().version()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.language.name")), is("java"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("process.runtime.name")), is("Java"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("process.runtime.version")), is(Runtime.version().toString()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.agent.name")), is("elasticsearch-otel-sdk"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.agent.version")), is(Build.current().version()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("telemetry.distro.name")), is("elasticsearch-otel-sdk"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("telemetry.distro.version")), is(Build.current().version()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.instance.id")), is("node-7"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("project.id")), is("abc-123"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("orchestrator.cluster.name")), is("es-prod-eu-west-1"));
    }

    public void testOtelSdkResourceOverride() {
        Settings settings = Settings.builder().put("telemetry.resource.service.name", "operator-supplied-name").build();

        Resource resource = OtelSdkResource.get(settings);

        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), is("operator-supplied-name"));
    }
}
