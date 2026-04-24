/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.api.logs.Severity.INFO;
import static io.opentelemetry.api.logs.Severity.WARN;
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

    public void testBatchLogIndexing() throws Exception {
        int numLogs = 100;
        for (int i = 0; i < numLogs; i++) {
            logger.logRecordBuilder().setBody("log message " + i).setSeverity(INFO).emit();
        }
        indexLogs();

        ObjectPath search = search("logs-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(numLogs));
    }

    public void testLogWithAttributes() throws Exception {
        logger.logRecordBuilder()
            .setBody("request handled")
            .setSeverity(WARN)
            .setSeverityText("WARN")
            .setAttribute(stringKey("http.method"), "GET")
            .setAttribute(AttributeKey.longKey("http.status_code"), 404L)
            .emit();
        indexLogs();

        ObjectPath search = search("logs-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(search.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("body.text"), equalTo("request handled"));
        assertThat(source.evaluate("severity_text"), equalTo("WARN"));
        assertThat(source.evaluate("severity_number"), equalTo(WARN.getSeverityNumber()));
        assertThat(source.evaluate("attributes.http\\.method"), equalTo("GET"));
        assertThat(source.evaluate("attributes.http\\.status_code"), equalTo(404));
        assertThat(source.evaluate("resource.attributes.service\\.name"), equalTo("elasticsearch"));
    }

    public void testDataStreamRouting() throws Exception {
        logger.logRecordBuilder()
            .setBody("routed log")
            .setSeverity(INFO)
            .setAttribute(stringKey("data_stream.dataset"), "myapp")
            .setAttribute(stringKey("data_stream.namespace"), "production")
            .emit();
        indexLogs();

        ObjectPath search = search("logs-myapp.otel-production");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(search.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("data_stream.type"), equalTo("logs"));
        assertThat(source.evaluate("data_stream.dataset"), equalTo("myapp.otel"));
        assertThat(source.evaluate("data_stream.namespace"), equalTo("production"));
    }

    public void testScopeIsIndexed() throws Exception {
        logger.logRecordBuilder().setBody("scoped log").setSeverity(INFO).emit();
        indexLogs();

        ObjectPath search = search("logs-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = new ObjectPath(search.evaluate("hits.hits.0._source"));
        assertThat(source.evaluate("scope.name"), equalTo(getClass().getSimpleName()));
    }

    private void indexLogs() throws IOException {
        var result = loggerProvider.forceFlush().join(TEST_REQUEST_TIMEOUT.millis(), MILLISECONDS);
        assertThat(result.isSuccess(), equalTo(true));
        refresh("logs-*.otel-*");
    }

}
