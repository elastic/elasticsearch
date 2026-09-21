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
 * Measures native stage 1 alone — structural indexing and UTF-8 validation — with no walk, no
 * handler, and none of {@code SimdJsonParser}'s document-window bookkeeping.
 *
 * <h2>Why this benchmark exists</h2>
 *
 * <p>{@link SimdJsonParserBenchmark} and {@code StringFieldParsingBenchmark} both go through
 * {@code JsonDocumentParser.parseDocument}, which is {@code stage1} followed by
 * {@code walkDocument}, so they report only the <em>sum</em> of the two. That is the right number
 * for a go/no-go decision, but it cannot distinguish "neither side moved" from "stage 1 got slower
 * by as much as the walker got faster" — and which of those is true decides whether a result
 * generalizes beyond the shapes measured.
 *
 * <p>That distinction matters for any change shifting work across the stage-1/walk boundary.
 * Recording more per-string information in the index is paid on every document, including ones
 * whose strings a handler never reads, while the saving lands only in the walker. Read this
 * alongside the end-to-end benchmarks, not instead of them.
 *
 * <p>The setup summary prints index entries per document per shape, which is the input that
 * explains a throughput difference rather than a measurement of it — {@code small_sparse} gains
 * the most entries per byte, having many short keys.
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
     * All three shapes by default: unlike the end-to-end benchmark this one is cheap, and the
     * shapes differ in exactly the dimension that drives index density — {@code clickbench_flat}
     * has few long string values, {@code small_sparse} many short keys, {@code otel_nested} the
     * deepest structure.
     */
    @Param({ "clickbench_flat", "small_sparse", "otel_nested" })
    private String shape;

    @Param({ "0" })
    private int escapePercent;

    private byte[][] docs;
    private StructuralIndexer indexer;
    private BitIndexes bitIndexes;

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
}
