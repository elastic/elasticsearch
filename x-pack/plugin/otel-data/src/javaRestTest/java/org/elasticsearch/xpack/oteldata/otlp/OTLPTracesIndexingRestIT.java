/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;

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
            .addHeader("Authorization", "ApiKey " + createApiKey("traces-*"))
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

    private void indexTraces() throws IOException {
        var result = tracerProvider.forceFlush().join(TEST_REQUEST_TIMEOUT.millis(), MILLISECONDS);
        assertThat(result.isSuccess(), equalTo(true));
        refresh("traces-*.otel-*");
    }

}
