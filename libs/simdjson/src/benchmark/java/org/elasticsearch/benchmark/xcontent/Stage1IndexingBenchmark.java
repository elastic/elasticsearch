/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.simdjson.JsonDocumentParser;
import org.elasticsearch.simdjson.SimdJsonParserPool;
import org.elasticsearch.simdjson.internal.StructuralIndexer;
import org.elasticsearch.simdjson.internal.parsers.BitIndexes;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Measures native stage 1 (structural indexing and UTF-8 validation), in isolation or with
 * a walk (but with a no-op handler).
 *
 * <p><strong>Running.</strong>
 * <pre>{@code
 * ./gradlew :libs:simdjson:benchmark --args "Stage1IndexingBenchmark \
 *   -rf json -rff build/jmh-result.json"
 *
 * # escapes add an index entry per backslash:
 * ./gradlew :libs:simdjson:benchmark --args "Stage1IndexingBenchmark -p escapePercent=0,10"
 * }</pre>
 */
@Fork(value = 1, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class Stage1IndexingBenchmark {

    @Param({ "1000" })
    private int docCount;

    @Param({ "42" })
    private long seed;

    /**
     * We want to have all three shapes by default: unlike the end-to-end benchmark this one is
     * cheap, and the shapes differ in exactly the dimension that drives index density:
     * {@code analytics} is wide and flat with escape-dense URL strings, {@code small_sparse}
     * has many short keys, {@code otel_nested} the deepest structure.
     */
    @Param({ "analytics", "small_sparse", "otel_nested" })
    private String shape;

    @Param({ "0" })
    private int escapePercent;

    private byte[][] docs;
    private StructuralIndexer indexer;
    private BitIndexes bitIndexes;
    private JsonDocumentParser docParser;
    private final NoOpHandler noopHandler = new NoOpHandler();

    @Setup
    public void setUp() {
        BenchmarkLogging.configure();
        Random random = new Random(seed);
        docs = new byte[docCount][];
        int maxLen = 0;
        long totalLen = 0;
        for (int i = 0; i < docCount; i++) {
            docs[i] = SimdJsonParserBenchmark.generateDoc(random, shape, i, escapePercent).getBytes(UTF_8);
            maxLen = Math.max(maxLen, docs[i].length);
            totalLen += docs[i].length;
        }
        indexer = new StructuralIndexer(Math.max(maxLen, 4096));
        bitIndexes = new BitIndexes(maxLen + 1);
        docParser = new SimdJsonParserPool(Math.max(maxLen, 4096)).forCurrentThread();
        printSetupSummary(totalLen);
    }

    @SuppressForbidden(reason = "index density per shape is what makes a throughput difference readable")
    private void printSetupSummary(long totalLen) {
        long totalEntries = 0;
        for (byte[] doc : docs) {
            indexer.index(doc, doc.length, bitIndexes);
            totalEntries += bitIndexes.writeCount();
        }
        System.out.printf(
            Locale.ROOT,
            "[setup] shape=%s escapePercent=%d docs=%d avgBytes=%d avgIndexEntries=%d entriesPerByte=%.3f%n",
            shape,
            escapePercent,
            docCount,
            totalLen / docCount,
            totalEntries / docCount,
            (double) totalEntries / totalLen
        );
    }

    @TearDown
    public void tearDown() {
        indexer.close();
    }

    @Benchmark
    public int stage1Only() {
        int entries = 0;
        for (byte[] doc : docs) {
            indexer.index(doc, doc.length, bitIndexes);
            entries += bitIndexes.writeCount();
        }
        return entries;
    }

    /**
     * Stage 1 plus the walk, with a handler that records nothing.
     */
    @Benchmark
    public long stage1AndWalk() {
        for (byte[] doc : docs) {
            docParser.parseDocument(doc, doc.length, noopHandler);
        }
        docParser.publishFieldNames();
        return noopHandler.checksum;
    }
}
