/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark to measure indexing performance of keyword fields. Used to measure performance impact of skipping
 * UTF-8 to UTF-16 conversion during document parsing.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
public class OptimizedTextBenchmark {
    static {
        // For Elasticsearch900Lucene101Codec:
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
        LogConfigurator.setNodeName("test");
    }

    /**
     * Total number of documents to index.
     */
    @Param("1048576")
    private int nDocs;

    private MapperService mapperService;
    private SourceToParse[] sources;

    private String randomValue(int length) {
        final String CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return builder.toString();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        mapperService = MapperServiceFactory.create("""
            {
                "_doc": {
                    "dynamic": false,
                    "properties": {
                        "field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """);

        sources = new SourceToParse[nDocs];
        for (int i = 0; i < nDocs; i++) {
            XContentBuilder b = XContentFactory.jsonBuilder();
            b.startObject().field("field", randomValue(8)).endObject();
            sources[i] = new SourceToParse(UUIDs.randomBase64UUID(), BytesReference.bytes(b), XContentType.JSON);
        }
    }

    @Benchmark
    public void indexDocuments(final Blackhole bh) {
        final var mapper = mapperService.documentMapper();
        for (int i = 0; i < nDocs; i++) {
            bh.consume(mapper.parse(sources[i]));
        }
    }
}
