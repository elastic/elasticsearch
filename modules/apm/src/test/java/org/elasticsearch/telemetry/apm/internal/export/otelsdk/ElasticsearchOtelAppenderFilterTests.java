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
import org.apache.logging.log4j.message.SimpleMessage;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class ElasticsearchOtelAppenderFilterTests extends ESTestCase {

    private InMemoryLogRecordExporter exporter;
    private SdkLoggerProvider provider;
    private ElasticsearchOtelAppender appender;

    @Before
    public void setup() {
        exporter = InMemoryLogRecordExporter.create();
        provider = SdkLoggerProvider.builder().addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        appender = new ElasticsearchOtelAppender("test-filter", sdk);
        appender.start();
    }

    @After
    public void teardown() {
        appender.stop();
        provider.close();
    }

    private void emit(String text) {
        appender.append(Log4jLogEvent.newBuilder().setLoggerName("test").setLevel(Level.INFO).setMessage(new SimpleMessage(text)).build());
    }

    public void testFilterDropsEvents() {
        appender.addFilter(event -> null);
        emit("dropped");
        assertThat(exporter.getFinishedLogRecordItems(), hasSize(0));
    }

    public void testFilterRewritesEvent() {
        appender.addFilter(
            event -> Log4jLogEvent.newBuilder()
                .setLoggerName(event.getLoggerName())
                .setLevel(event.getLevel())
                .setMessage(new SimpleMessage("rewritten"))
                .build()
        );
        emit("original");
        List<LogRecordData> records = exporter.getFinishedLogRecordItems();
        assertThat(records, hasSize(1));
        assertThat(records.getFirst().getBodyValue().asString(), equalTo("rewritten"));
    }

    public void testFilterRewritesAttribute() {
        appender.addFilter(event -> {
            if (event.getMessage() instanceof ESLogMessage msg) {
                return Log4jLogEvent.newBuilder()
                    .setLoggerName(event.getLoggerName())
                    .setLevel(event.getLevel())
                    .setMessage(new ESLogMessage().field("k", "filtered-value"))
                    .build();
            }
            return event;
        });
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
    }

    public void testFilterChainShortCircuits() {
        AtomicInteger secondFilterCalls = new AtomicInteger(0);
        appender.addFilter(event -> null);
        appender.addFilter(event -> {
            secondFilterCalls.incrementAndGet();
            return event;
        });
        emit("any");
        assertThat(exporter.getFinishedLogRecordItems(), hasSize(0));
        assertThat(secondFilterCalls.get(), equalTo(0));
    }
}
