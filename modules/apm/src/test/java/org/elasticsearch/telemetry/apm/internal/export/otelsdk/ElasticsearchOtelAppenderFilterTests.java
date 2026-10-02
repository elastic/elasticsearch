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
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.telemetry.TelemetryLogEventFilter;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class ElasticsearchOtelAppenderFilterTests extends ESTestCase {

    private InMemoryLogRecordExporter exporter;
    private SdkLoggerProvider provider;

    @Before
    public void setup() {
        exporter = InMemoryLogRecordExporter.create();
        provider = SdkLoggerProvider.builder().addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
    }

    @After
    public void teardown() {
        provider.close();
    }

    private ElasticsearchOtelAppender makeAppender(TelemetryLogEventFilter filter) {
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        ElasticsearchOtelAppender appender = new ElasticsearchOtelAppender("test-filter", sdk, filter);
        appender.start();
        return appender;
    }

    private void emit(ElasticsearchOtelAppender appender, String text) {
        appender.append(
            Log4jLogEvent.newBuilder().setLoggerName("test").setLevel(Level.INFO).setMessage(new ESLogMessage().field("data", text)).build()
        );
    }

    public void testFilterDropsEvents() {
        ElasticsearchOtelAppender appender = makeAppender(event -> null);
        try {
            emit(appender, "dropped");
            assertThat(exporter.getFinishedLogRecordItems(), hasSize(0));
        } finally {
            appender.stop();
        }
    }

    public void testNoFilterPassesEvents() {
        ElasticsearchOtelAppender appender = makeAppender(null);
        try {
            emit(appender, "pass");
            assertThat(exporter.getFinishedLogRecordItems(), hasSize(1));
        } finally {
            appender.stop();
        }
    }

    public void testFilterRewritesAttribute() {
        ElasticsearchOtelAppender appender = makeAppender(data -> Map.of("k", "filtered-value"));
        try {
            appender.append(
                Log4jLogEvent.newBuilder()
                    .setLoggerName("test")
                    .setLevel(Level.INFO)
                    .setMessage(new ESLogMessage().field("k", "original-value"))
                    .build()
            );
            List<LogRecordData> records = exporter.getFinishedLogRecordItems();
            assertThat(records, hasSize(1));
            assertThat(records.getFirst().getAttributes().get(AttributeKey.stringKey("k")), equalTo("filtered-value"));
        } finally {
            appender.stop();
        }
    }
}
