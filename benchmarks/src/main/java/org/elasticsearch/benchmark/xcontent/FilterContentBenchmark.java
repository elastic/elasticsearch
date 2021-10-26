/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.filtering.FilterPath;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FilterContentBenchmark {

    @Param({ "cluster_stats", "index_stats", "node_stats" })
    private String type;

    @Param({ "10_field", "half_field", "all_field" })
    private String fieldCount;

    private BytesReference source;
    private FilterPath[] includesFilters;

    @Setup
    public void setup() throws IOException {
        String sourceFile;
        switch (type) {
            case "cluster_stats":
                sourceFile = "monitor_cluster_stats.json";
                break;
            case "index_stats":
                sourceFile = "monitor_index_stats.json";
                break;
            case "node_stats":
                sourceFile = "monitor_node_stats.json";
                break;
            default:
                throw new IllegalArgumentException("Unknown type [" + type + "]");
        }
        source = readSource(sourceFile);

        Map<String, Object> flattenMap = Maps.flatten(XContentHelper.convertToMap(source, true, XContentType.JSON).v2(), false, true);
        Set<String> keys = flattenMap.keySet();

        switch (fieldCount) {
            case "10_field": {
                AtomicInteger count = new AtomicInteger();
                Set<String> filterKeys = keys.stream()
                    .filter(key -> count.getAndIncrement() % 5 == 0)
                    .limit(10)
                    .collect(Collectors.toSet());
                includesFilters = FilterPath.compile(filterKeys);
            }
                break;
            case "half_field": {
                AtomicInteger count = new AtomicInteger();
                Set<String> halfKeys = keys.stream().filter(key -> count.getAndIncrement() % 2 == 0).collect(Collectors.toSet());
                includesFilters = FilterPath.compile(halfKeys);
            }
                break;
            case "all_field":
                includesFilters = FilterPath.compile(keys);
                break;
            default:
                throw new IllegalArgumentException("Unknown type [" + type + "]");
        }
    }

    @Benchmark
    public BytesReference benchmark() throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), os);
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        source.streamInput(),
                        includesFilters,
                        null
                    )
            ) {
                builder.copyCurrentStructure(parser);
                return BytesReference.bytes(builder);
            }
        }
    }

    private BytesReference readSource(String fileName) throws IOException {
        return Streams.readFully(FilterContentBenchmark.class.getResourceAsStream(fileName));
    }
}
