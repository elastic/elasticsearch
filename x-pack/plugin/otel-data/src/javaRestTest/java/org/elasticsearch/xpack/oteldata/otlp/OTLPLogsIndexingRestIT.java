/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;

public class OTLPLogsIndexingRestIT extends AbstractOTLPIndexingRestIT {

    private SdkLoggerProvider loggerProvider;
    private Logger logger;

    @Override
    protected String otlpEndpointPath() {
        return "/_otlp/v1/logs";
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        OtlpHttpLogRecordExporter exporter = OtlpHttpLogRecordExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/logs")
            .addHeader("Authorization", "ApiKey " + createApiKey("logs-*"))
            .build();
        loggerProvider = SdkLoggerProvider.builder()
            .setResource(TEST_RESOURCE)
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporter).build())
            .build();
        logger = loggerProvider.get(getClass().getSimpleName());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (loggerProvider != null) {
            loggerProvider.close();
        }
    }

    private void indexLogs() throws IOException {
        var result = loggerProvider.forceFlush().join(TEST_REQUEST_TIMEOUT.millis(), MILLISECONDS);
        assertThat(result.isSuccess(), equalTo(true));
        refresh("logs-*.otel-*");
    }

}
