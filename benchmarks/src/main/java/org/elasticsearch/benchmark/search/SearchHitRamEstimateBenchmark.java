/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the runtime overhead of {@link SearchHit#ramBytesUsed()} (backed by
 * {@code SearchHitRamUsageEstimator}). On the coordinator this method is invoked once per
 * deserialized hit during a chunked fetch to charge the request circuit breaker, so its cost is
 * paid on the hot fetch path. This benchmark reports the per-hit estimation latency across a matrix
 * of hit shapes (field count, value type, source size, inner hits) to quantify that overhead.
 *
 * <p>Unlike {@code SearchHitRamAccountingBenchmark}, which focuses on the accuracy of the estimate
 * versus the real retained heap, this benchmark isolates pure execution time.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
@SuppressWarnings("unused")
public class SearchHitRamEstimateBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Number of chunk-sized hits estimated per {@link #estimateChunk} invocation. */
    private static final int CHUNK_SIZE = 128;

    @Param({ "0", "1", "10", "100", "1000" })
    public int fieldCount;

    @Param({ "1" })
    public int valuesPerField;

    @Param({ "keyword", "long", "text" })
    public String valueType;

    @Param({ "0", "512" })
    public int sourceBytes;

    @Param({ "0", "5" })
    public int innerHitCount;

    private SearchHit singleHit;
    private SearchHit[] chunk;

    @Setup(Level.Trial)
    public void setup() {
        singleHit = buildHit(0);
        chunk = new SearchHit[CHUNK_SIZE];
        for (int i = 0; i < CHUNK_SIZE; i++) {
            chunk[i] = buildHit(i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        singleHit = null;
        chunk = null;
    }

    /**
     * Per-hit estimation latency. The returned value is consumed by JMH so the call cannot be
     * eliminated as dead code.
     */
    @Benchmark
    public long estimateSingleHit() {
        return singleHit.ramBytesUsed();
    }

    /**
     * Cost of estimating a full chunk of hits, mirroring the coordinator summing
     * {@code ramBytesUsed()} over the hits in a received chunk before charging the breaker.
     */
    @Benchmark
    public void estimateChunk(Blackhole bh) {
        long total = 0L;
        for (SearchHit hit : chunk) {
            total += hit.ramBytesUsed();
        }
        bh.consume(total);
    }

    private SearchHit buildHit(int seq) {
        SearchHit hit = SearchHit.unpooled(seq, "id-" + seq);
        if (sourceBytes > 0) {
            hit.sourceRef(new BytesArray(new byte[sourceBytes]));
        }
        if (fieldCount > 0) {
            Map<String, DocumentField> docFields = new HashMap<>();
            for (int f = 0; f < fieldCount; f++) {
                String name = "field_" + f;
                docFields.put(name, new DocumentField(name, buildValues(seq, f)));
            }
            hit.addDocumentFields(docFields, Map.of());
        }
        if (innerHitCount > 0) {
            SearchHit[] inner = new SearchHit[innerHitCount];
            for (int j = 0; j < innerHitCount; j++) {
                SearchHit innerHit = SearchHit.unpooled(j, "inner-" + seq + "-" + j);
                if (sourceBytes > 0) {
                    innerHit.sourceRef(new BytesArray(new byte[sourceBytes]));
                }
                innerHit.setDocumentField(new DocumentField("inner_field", buildValues(seq, j)));
                inner[j] = innerHit;
            }
            hit.setInnerHits(Map.of("nested", new SearchHits(inner, new TotalHits(innerHitCount, TotalHits.Relation.EQUAL_TO), 1f)));
        }
        return hit;
    }

    private List<Object> buildValues(int seq, int field) {
        List<Object> values = new ArrayList<>(valuesPerField);
        for (int v = 0; v < valuesPerField; v++) {
            switch (valueType) {
                case "keyword" -> values.add("kw-" + seq + "-" + field + "-" + v);
                case "long" -> values.add(1_000_000L + (long) seq * 31 + (long) field * 7 + v);
                case "text" -> values.add(text(seq, field, v));
                default -> throw new IllegalArgumentException("unknown valueType [" + valueType + "]");
            }
        }
        return values;
    }

    private static String text(int seq, int field, int v) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("doc ").append(seq).append(" field ").append(field).append(" value ").append(v).append(' ');
        while (sb.length() < 120) {
            sb.append("lorem ipsum dolor sit amet ");
        }
        return sb.substring(0, 120);
    }
}
