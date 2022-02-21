/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexAbstraction.DataStream.TS_EXTRACT_CONFIG;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FilterTimestampBenchmark {
    @Param({ "first", "last" })
    private String type;

    private BytesReference source;
    private XContentParserConfiguration parserConfig;

    @Setup
    public void setup() throws IOException {
        String sourceFile = switch (type) {
            case "first" -> "monitor_cluster_stats.json";
            case "last" -> "monitor_cluster_stats_last_timestamp.json";
            default -> throw new IllegalArgumentException("Unknown type [" + type + "]");
        };
        source = Streams.readFully(FilterTimestampBenchmark.class.getResourceAsStream(sourceFile));
        parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
            Set.of("timestamp"),
            null,
            false
        );
    }

    @Benchmark
    public String filter() {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig, source.streamInput())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            switch (parser.nextToken()) {
                case VALUE_STRING:
                    return parser.text();
                case VALUE_NUMBER:
                    return String.valueOf(parser.longValue());
                default:
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(
                            Locale.ROOT,
                            "Failed to parse object: expecting token of type [%s] or [%s] but found [%s]",
                            XContentParser.Token.VALUE_STRING,
                            XContentParser.Token.VALUE_NUMBER,
                            parser.currentToken()
                        )
                    );
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error extracting timestamp: " + e.getMessage(), e);
        }
    }
}
