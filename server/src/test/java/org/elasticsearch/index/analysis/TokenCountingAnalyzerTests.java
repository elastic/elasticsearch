/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TokenCountingAnalyzerTests extends MapperServiceTestCase {

    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(telemetryPlugin);
    }

    @Override
    public void testFieldHasValue() {}

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {}

    public void testRecordsTokenCountAboveThreshold() throws IOException {
        var metrics = createTestMapperMetrics().tokenCountingMetrics();
        int tokenCount = randomIntBetween(1000, 2000);
        String input = IntStream.range(0, tokenCount).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, metrics);
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }

        var measurements = telemetryPlugin.getLongHistogramMeasurement(TokenCountingMetrics.FIELD_TOKEN_COUNT);
        assertEquals(1, measurements.size());
        assertEquals(tokenCount, measurements.get(0).getLong());
    }

    public void testRecordsPerFieldValue() throws IOException {
        var metrics = createTestMapperMetrics().tokenCountingMetrics();

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, metrics);

            // First field value: 1001 tokens (above threshold)
            String input1 = IntStream.range(0, 1001).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));
            try (TokenStream ts = analyzer.tokenStream("field", input1)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }

            // Second field value (same field name, reused components): 1500 tokens (above threshold)
            String input2 = IntStream.range(0, 1500).mapToObj(i -> "term" + i).collect(Collectors.joining(" "));
            try (TokenStream ts = analyzer.tokenStream("field", input2)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }

        var measurements = telemetryPlugin.getLongHistogramMeasurement(TokenCountingMetrics.FIELD_TOKEN_COUNT);
        assertEquals(2, measurements.size());
        assertEquals(1001, measurements.get(0).getLong());
        assertEquals(1500, measurements.get(1).getLong());
    }

    public void testDoesNotRecordBelowThreshold() throws IOException {
        var metrics = createTestMapperMetrics().tokenCountingMetrics();
        // 999 tokens — below the 1000 threshold
        String input = IntStream.range(0, 999).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, metrics);
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }

        var measurements = telemetryPlugin.getLongHistogramMeasurement(TokenCountingMetrics.FIELD_TOKEN_COUNT);
        assertEquals(0, measurements.size());
    }

    public void testNoopMetricsDoesNotRecord() throws IOException {
        int tokenCount = randomIntBetween(1000, 2000);
        String input = IntStream.range(0, tokenCount).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, TokenCountingMetrics.NOOP);
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(tokenCount, count);
            }
        }

        // NOOP metrics records to MeterRegistry.NOOP, so nothing shows up in the test telemetry plugin
        var measurements = telemetryPlugin.getLongHistogramMeasurement(TokenCountingMetrics.FIELD_TOKEN_COUNT);
        assertEquals(0, measurements.size());
    }
}
