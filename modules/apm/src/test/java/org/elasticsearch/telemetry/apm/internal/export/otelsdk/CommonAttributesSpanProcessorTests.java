/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CommonAttributesSpanProcessorTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final CommonAttributesSpanProcessor processor = new CommonAttributesSpanProcessor(threadContext);
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(processor)
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build();

    @After
    public void closeTracerProvider() {
        tracerProvider.close();
    }

    public void testAddsTheNamesToTheSpans() {
        processor.setClusterName("test-cluster");
        processor.setNodeName("test-node");

        final SpanData span = startAndEnd("name1");

        assertThat(span.getAttributes().get(stringKey("es.cluster.name")), equalTo("test-cluster"));
        assertThat(span.getAttributes().get(stringKey("es.node.name")), equalTo("test-node"));
    }

    public void testSpansStartedBeforeTheNamesAreSetHaveNone() {
        final SpanData before = startAndEnd("before");
        processor.setClusterName("test-cluster");
        processor.setNodeName("test-node");
        final SpanData after = startAndEnd("after");

        assertThat(before.getAttributes().get(stringKey("es.cluster.name")), nullValue());
        assertThat(before.getAttributes().get(stringKey("es.node.name")), nullValue());
        assertThat(after.getAttributes().get(stringKey("es.cluster.name")), equalTo("test-cluster"));
    }

    public void testAddsTheOpaqueIdAndTheProjectIdOfTheRequest() {
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, "opaque-1");
        threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, "project-1");

        final SpanData span = startAndEnd("name1");

        assertThat(span.getAttributes().get(stringKey("es.x-opaque-id")), equalTo("opaque-1"));
        assertThat(span.getAttributes().get(stringKey("project.id")), equalTo("project-1"));
    }

    public void testRequestAttributesAreOmittedWithoutTheHeaders() {
        final SpanData span = startAndEnd("name1");

        assertThat(span.getAttributes().get(stringKey("es.x-opaque-id")), nullValue());
        assertThat(span.getAttributes().get(stringKey("project.id")), nullValue());
    }

    public void testRequestAttributesAreReadWhenTheSpanStarts() {
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, "opaque-1");
        final SpanData first = startAndEnd("first");
        threadContext.putHeaderAllowOverwrite(Task.X_OPAQUE_ID_HTTP_HEADER, "opaque-2");
        final SpanData second = startAndEnd("second");

        assertThat(first.getAttributes().get(stringKey("es.x-opaque-id")), equalTo("opaque-1"));
        assertThat(second.getAttributes().get(stringKey("es.x-opaque-id")), equalTo("opaque-2"));
    }

    private SpanData startAndEnd(String name) {
        tracerProvider.get("test").spanBuilder(name).startSpan().end();
        return exporter.getFinishedSpanItems().stream().filter(span -> span.getName().equals(name)).findFirst().orElseThrow();
    }
}
