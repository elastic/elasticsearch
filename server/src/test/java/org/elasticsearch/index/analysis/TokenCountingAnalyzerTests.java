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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TokenCountingAnalyzerTests extends ESTestCase {

    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

    public void testRecordsTokenCount() throws IOException {
        var metrics = createMetrics();
        int tokenCount = randomIntBetween(1, 2000);
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
        var metrics = createMetrics();
        int firstCount = randomIntBetween(1, 500);
        int secondCount = randomIntBetween(1, 500);

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, metrics);

            String input1 = IntStream.range(0, firstCount).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));
            try (TokenStream ts = analyzer.tokenStream("field", input1)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }

            // Second field value (same field name, reused components)
            String input2 = IntStream.range(0, secondCount).mapToObj(i -> "term" + i).collect(Collectors.joining(" "));
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
        assertEquals(firstCount, measurements.get(0).getLong());
        assertEquals(secondCount, measurements.get(1).getLong());
    }

    public void testNoopMetricsDoesNotRecord() throws IOException {
        int tokenCount = randomIntBetween(1, 100);
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

    private TokenCountingMetrics createMetrics() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        var meterRegistry = telemetryPlugin.getTelemetryProvider(settings).getMeterRegistry();
        return new TokenCountingMetrics(meterRegistry);
    }
}
