/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FilterContentBenchmark {

    @Param({ "cluster_stats", "index_stats", "node_stats" })
    private String type;

    @Param({ "10_field", "half_field", "all_field", "wildcard_field", "10_wildcard_field" })
    private String fieldCount;

    @Param({ "true" })
    private boolean inclusive;

    private BytesReference source;
    private XContentParserConfiguration parserConfig;
    private Set<String> filters;
    private XContentParserConfiguration parserConfigMatchDotsInFieldNames;

    @Setup
    public void setup() throws IOException {
        String sourceFile = switch (type) {
            case "cluster_stats" -> "monitor_cluster_stats.json";
            case "index_stats" -> "monitor_index_stats.json";
            case "node_stats" -> "monitor_node_stats.json";
            default -> throw new IllegalArgumentException("Unknown type [" + type + "]");
        };
        source = readSource(sourceFile);
        filters = buildFilters();
        parserConfig = buildParseConfig(false);
        parserConfigMatchDotsInFieldNames = buildParseConfig(true);
    }

    private Set<String> buildFilters() {
        Map<String, Object> flattenMap = Maps.flatten(XContentHelper.convertToMap(source, true, XContentType.JSON).v2(), false, true);
        Set<String> keys = flattenMap.keySet();
        AtomicInteger count = new AtomicInteger();
        return switch (fieldCount) {
            case "10_field" -> keys.stream().filter(key -> count.getAndIncrement() % 5 == 0).limit(10).collect(Collectors.toSet());
            case "half_field" -> keys.stream().filter(key -> count.getAndIncrement() % 2 == 0).collect(Collectors.toSet());
            case "all_field" -> new HashSet<>(keys);
            case "wildcard_field" -> new HashSet<>(Arrays.asList("*stats"));
            case "10_wildcard_field" -> Set.of(
                "*stats.nodes*",
                "*stats.ind*",
                "*sta*.shards",
                "*stats*.xpack",
                "*stats.*.segments",
                "*stat*.*.data*",
                inclusive ? "*stats.**.request_cache" : "*stats.*.request_cache",
                inclusive ? "*stats.**.stat" : "*stats.*.stat",
                inclusive ? "*stats.**.threads" : "*stats.*.threads",
                "*source_node.t*"
            );
            default -> throw new IllegalArgumentException("Unknown type [" + type + "]");
        };
    }

    @Benchmark
    public BytesReference filterWithParserConfigCreated() throws IOException {
        return filter(this.parserConfig);
    }

    @Benchmark
    public BytesReference filterWithParserConfigCreatedMatchDotsInFieldNames() throws IOException {
        return filter(this.parserConfigMatchDotsInFieldNames);
    }

    @Benchmark
    public BytesReference filterWithNewParserConfig() throws IOException {
        XContentParserConfiguration contentParserConfiguration = buildParseConfig(false);
        return filter(contentParserConfiguration);
    }

    @Benchmark
    public BytesReference filterWithMap() throws IOException {
        Map<String, Object> sourceMap = XContentHelper.convertToMap(source, false).v2();
        String[] includes;
        String[] excludes;
        if (inclusive) {
            includes = filters.toArray(Strings.EMPTY_ARRAY);
            excludes = null;
        } else {
            includes = null;
            excludes = filters.toArray(Strings.EMPTY_ARRAY);
        }
        Map<String, Object> filterMap = XContentMapValues.filter(sourceMap, includes, excludes);
        return Source.fromMap(filterMap, XContentType.JSON).internalSourceRef();
    }

    @Benchmark
    public BytesReference filterWithBuilder() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, source.length()));
        Set<String> includes;
        Set<String> excludes;
        if (inclusive) {
            includes = filters;
            excludes = Set.of();
        } else {
            includes = Set.of();
            excludes = filters;
        }
        XContentBuilder builder = new XContentBuilder(
            XContentType.JSON.xContent(),
            streamOutput,
            includes,
            excludes,
            XContentType.JSON.toParsedMediaType()
        );
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, source.streamInput())) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private XContentParserConfiguration buildParseConfig(boolean matchDotsInFieldNames) {
        Set<String> includes;
        Set<String> excludes;
        if (inclusive) {
            includes = filters;
            excludes = null;
        } else {
            includes = null;
            excludes = filters;
        }
        return XContentParserConfiguration.EMPTY.withFiltering(includes, excludes, matchDotsInFieldNames);
    }

    private BytesReference filter(XContentParserConfiguration contentParserConfiguration) throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), os);
            try (XContentParser parser = XContentType.JSON.xContent().createParser(contentParserConfiguration, source.streamInput())) {
                if (parser.nextToken() != null) {
                    builder.copyCurrentStructure(parser);
                }
                return BytesReference.bytes(builder);
            }
        }
    }

    private BytesReference readSource(String fileName) throws IOException {
        return Streams.readFully(FilterContentBenchmark.class.getResourceAsStream(fileName));
    }
}
