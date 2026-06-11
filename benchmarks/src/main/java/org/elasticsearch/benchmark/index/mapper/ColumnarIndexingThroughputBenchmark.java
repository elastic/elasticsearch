/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
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

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures indexing throughput (document parsing, including {@code SourceFieldMapper#postParse})
 * across source modes. The {@code columnar_stored} mode performs a full in-memory Lucene index
 * write and synthetic-source reconstruction per document inside {@code postParse}, which is the
 * suspected cause of the indexing throughput regression. Running both modes side-by-side makes
 * the overhead directly visible.
 *
 * <p>The benchmark pre-generates a pool of documents in {@code @Setup} and then loops over them
 * in each invocation, so the reported average time covers {@code docCount} full parse + postParse
 * cycles per measurement sample.
 */
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ColumnarIndexingThroughputBenchmark {

    // Mapping with synthetic-source-compatible fields (doc_values enabled on all leaf fields).
    private static final String MAPPING = """
        {
          "_doc": {
            "dynamic": false,
            "properties": {
              "host":       { "type": "keyword" },
              "message":    { "type": "keyword" },
              "bytes":      { "type": "long"    },
              "cpu":        { "type": "double"  },
              "@timestamp": { "type": "date"    },
              "client_ip":  { "type": "ip"      },
              "enabled":    { "type": "boolean" }
            }
          }
        }
        """;

    private static final String[] HOSTS = { "host-1", "host-2", "host-3", "host-4", "host-5" };
    private static final String[] MESSAGES = { "error occurred", "connected", "disconnected", "timeout", "retry" };

    /**
     * Source mode to benchmark. {@code synthetic} exercises the fast postParse no-op path;
     * {@code columnar_stored} exercises the expensive per-document in-memory Lucene reconstruction.
     */
    @Param({ "synthetic", "columnar_stored" })
    private String sourceMode;

    /** Number of documents parsed per benchmark invocation. */
    @Param({ "10000" })
    private int docCount;

    /** Seed for deterministic document generation across runs. */
    @Param({ "1600172297" })
    private long seed;

    private MapperService mapperService;
    private SourceToParse[] sources;

    @Setup
    public void setUp() {
        Utils.configureBenchmarkLogging();

        assert IndexMode.COLUMNAR.supportedSourceModes()
            .stream()
            .map(mode -> mode.toString().toLowerCase(Locale.ROOT))
            .toList()
            .contains(sourceMode);

        Settings.Builder settings = Settings.builder().put("index.mode", "columnar").put("index.mapping.source.mode", sourceMode);

        mapperService = MapperServiceFactory.create(MAPPING, List.of(), settings.build());

        Random random = new Random(seed);
        sources = new SourceToParse[docCount];
        for (int i = 0; i < docCount; i++) {
            sources[i] = new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray(generateDoc(random)), XContentType.JSON);
        }
    }

    /**
     * Parses all pre-generated documents and returns the total number of Lucene documents
     * produced. Returning a value prevents JMH from dead-code-eliminating the parse calls.
     * The hot path under test is {@code SourceFieldMapper#postParse}, which runs inside
     * {@code DocumentMapper#parse} for every document.
     */
    @Benchmark
    public long indexDocuments() {
        long total = 0;
        for (SourceToParse source : sources) {
            total += mapperService.documentMapper().parse(source).docs().size();
        }
        return total;
    }

    private static String generateDoc(Random random) {
        return String.format(
            """
                {
                  "host":       "%s",
                  "message":    "%s",
                  "bytes":      %d,
                  "cpu":        %.6f,
                  "@timestamp": "2024-01-%02dT%02d:%02d:%02dZ",
                  "client_ip":  "%d.%d.%d.%d",
                  "enabled":    %b
                }""",
            HOSTS[random.nextInt(HOSTS.length)],
            MESSAGES[random.nextInt(MESSAGES.length)],
            (long) (random.nextDouble() * 10_000_000L),
            random.nextDouble(),
            random.nextInt(1, 29),
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            random.nextInt(256),
            random.nextInt(256),
            random.nextInt(256),
            random.nextInt(256),
            random.nextBoolean()
        );
    }
}
