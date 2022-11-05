/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.search.fetch.subphase;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
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
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SourceFilteringBenchmark {

    private BytesReference sourceBytes;
    private SourceFilter filter;

    @Param({ "tiny", "short", "one_4k_field", "one_4m_field" })
    private String source;
    @Param({ "message" })
    private String includes;
    @Param({ "" })
    private String excludes;

    @Setup
    public void setup() throws IOException {
        sourceBytes = switch (source) {
            case "tiny" -> new BytesArray("{\"message\": \"short\"}");
            case "short" -> read300BytesExample();
            case "one_4k_field" -> buildBigExample("huge".repeat(1024));
            case "one_4m_field" -> buildBigExample("huge".repeat(1024 * 1024));
            default -> throw new IllegalArgumentException("Unknown source [" + source + "]");
        };
        FetchSourceContext fetchContext = FetchSourceContext.of(
            true,
            Strings.splitStringByCommaToArray(includes),
            Strings.splitStringByCommaToArray(excludes)
        );
        filter = fetchContext.filter();
    }

    private BytesReference read300BytesExample() throws IOException {
        return Streams.readFully(FetchSourcePhaseBenchmark.class.getResourceAsStream("300b_example.json"));
    }

    private BytesReference buildBigExample(String extraText) throws IOException {
        String bigger = read300BytesExample().utf8ToString();
        bigger = "{\"huge\": \"" + extraText + "\"," + bigger.substring(1);
        return new BytesArray(bigger);
    }

    // We want to compare map filtering with bytes filtering when the map has already
    // been parsed.

    @Benchmark
    public Source filterMap() {
        Source source = Source.fromBytes(sourceBytes);
        source.source();    // build map
        return filter.filterMap(source);
    }

    @Benchmark
    public Source filterBytes() {
        Source source = Source.fromBytes(sourceBytes);
        source.source();    // build map
        return filter.filterBytes(source);
    }
}
