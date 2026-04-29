/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class OTLPTracesIndexingRestIT extends AbstractOTLPIndexingRestIT {

    private SdkTracerProvider tracerProvider;
    private Tracer tracer;

    @Override
    protected String otlpEndpointPath() {
        return "/_otlp/v1/traces";
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + otlpEndpointPath())
            .addHeader("Authorization", "ApiKey " + createApiKey("traces-*", "logs-*"))
            .build();
        tracerProvider = SdkTracerProvider.builder()
            .setResource(TEST_RESOURCE)
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .build();
        tracer = tracerProvider.get(getClass().getSimpleName());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (tracerProvider != null) {
            tracerProvider.close();
        }
    }

    public void testBatchSpanIndexing() throws Exception {
        int numSpans = 25;
        for (int i = 0; i < numSpans; i++) {
            tracer.spanBuilder("span-" + i).startSpan().end();
        }

        indexTraces();

        ObjectPath search = search("traces-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(numSpans));
    }

    public void testSpanWithAttributes() throws Exception {
        var span = tracer.spanBuilder("GET /orders").setSpanKind(SpanKind.SERVER).startSpan();
        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        span.setAttribute("http.method", "GET");
        span.setAttribute("http.status_code", 404L);
        span.setStatus(StatusCode.ERROR, "not found");
        span.end();

        indexTraces();

        ObjectPath search = search("traces-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(search.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("name"), equalTo("GET /orders"));
        assertThat(source.evaluate("kind"), equalTo("Server"));
        assertThat(source.evaluate("trace_id"), equalTo(traceId));
        assertThat(source.evaluate("span_id"), equalTo(spanId));
        assertThat(source.evaluate("duration"), isA(Number.class));
        assertThat(source.evaluate("status.code"), equalTo("Error"));
        assertThat(source.evaluate("status.message"), equalTo("not found"));
        assertThat(source.evaluate("attributes.http\\.method"), equalTo("GET"));
        assertThat(source.evaluate("attributes.http\\.status_code"), equalTo(404));
        assertThat(source.evaluate("resource.attributes.service\\.name"), equalTo("elasticsearch"));
        assertThat(source.evaluate("scope.name"), equalTo(getClass().getSimpleName()));
    }

    public void testSpanEventsAreIndexedAsLogs() throws Exception {
        var span = tracer.spanBuilder("span-with-event").startSpan();
        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        span.addEvent("exception", Attributes.of(stringKey("event.attr.foo"), "event.attr.bar"));
        span.end();

        indexTraces();

        ObjectPath tracesSearch = search("traces-generic.otel-default");
        assertThat(tracesSearch.evaluate("hits.total.value"), equalTo(1));

        ObjectPath logsSearch = search("logs-generic.otel-default");
        assertThat(logsSearch.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(logsSearch.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("event_name"), equalTo("exception"));
        assertThat(source.evaluate("trace_id"), equalTo(traceId));
        assertThat(source.evaluate("span_id"), equalTo(spanId));
        assertThat(source.evaluate("attributes.event\\.attr\\.foo"), equalTo("event.attr.bar"));
        assertThat(source.evaluate("attributes.event\\.name"), equalTo("exception"));
        assertThat(source.evaluate("data_stream.type"), equalTo("logs"));
    }

    public void testSpanEventsUseDocumentIdAttribute() throws Exception {
        var span = tracer.spanBuilder("span-with-event-id").startSpan();
        span.addEvent("exception", Attributes.of(stringKey("elasticsearch.document_id"), "span-event-doc-id"));
        span.end();

        indexTraces();

        ObjectPath logsSearch = search("logs-generic.otel-default");
        assertThat(logsSearch.evaluate("hits.total.value"), equalTo(1));
        assertThat(logsSearch.evaluate("hits.hits.0._id"), equalTo("span-event-doc-id"));
    }

    public void testDataStreamRouting() throws Exception {
        var span = tracer.spanBuilder("routed span").startSpan();
        span.setAttribute("data_stream.dataset", "checkout");
        span.setAttribute("data_stream.namespace", "production");
        span.end();

        indexTraces();

        ObjectPath search = search("traces-checkout.otel-production");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(search.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("data_stream.type"), equalTo("traces"));
        assertThat(source.evaluate("data_stream.dataset"), equalTo("checkout.otel"));
        assertThat(source.evaluate("data_stream.namespace"), equalTo("production"));
    }

    private void indexTraces() throws IOException {
        var result = tracerProvider.forceFlush().join(TEST_REQUEST_TIMEOUT.millis(), MILLISECONDS);
        assertThat(result.isSuccess(), equalTo(true));
        refresh("traces-*.otel-*");
        refresh("logs-*.otel-*");
    }

}
