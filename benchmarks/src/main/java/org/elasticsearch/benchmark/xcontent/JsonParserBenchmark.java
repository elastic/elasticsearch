/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class JsonParserBenchmark {
    private Map<String, BytesReference> sourceBytes;
    private BytesReference source;
    private Random random;
    private List<String> sourcesRandomized;

    final String[] sources = new String[] { "monitor_cluster_stats.json", "monitor_index_stats.json", "monitor_node_stats.json" };

    @Setup(Level.Iteration)
    public void randomizeSource() {
        sourcesRandomized = Arrays.asList(sources);
        Collections.shuffle(sourcesRandomized, random);
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        random = new Random();
        sourceBytes = Arrays.stream(sources).collect(Collectors.toMap(s -> s, s -> {
            try {
                return readSource(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Benchmark
    public void parseJson(Blackhole bh) throws IOException {
        sourcesRandomized.forEach(source -> {
            try {
                final XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, sourceBytes.get(source).streamInput());
                bh.consume(parser.mapOrdered());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private BytesReference readSource(String fileName) throws IOException {
        return Streams.readFully(JsonParserBenchmark.class.getResourceAsStream(fileName));
    }
}
