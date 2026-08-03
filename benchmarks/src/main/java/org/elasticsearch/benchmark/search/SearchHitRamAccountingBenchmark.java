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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
@SuppressWarnings("unused")
public class SearchHitRamAccountingBenchmark {

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private static final long TARGET_FIELD_OBJECTS = 1_000_000L;
    private static final int MIN_BATCH = 2_000;
    private static final int MAX_BATCH = 100_000;

    static {
        Utils.configureBenchmarkLogging();
    }

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

    private SearchHit[] timingHits;

    @Setup(Level.Trial)
    public void setup() {
        int batch = batchSize();

        SearchHit[] hits = new SearchHit[batch];
        long before = usedHeapAfterGc();
        for (int i = 0; i < batch; i++) {
            hits[i] = buildHit(i);
        }
        long after = usedHeapAfterGc();

        long actualTotal = after - before;
        long estimateTotal = 0L;
        for (SearchHit hit : hits) {
            estimateTotal += hit.ramBytesUsed();
        }
        Reference.reachabilityFence(hits);

        double actualPerHit = (double) actualTotal / batch;
        double estimatePerHit = (double) estimateTotal / batch;
        double ratio = actualPerHit == 0 ? Double.NaN : estimatePerHit / actualPerHit;

        System.out.println(
            String.format(
                Locale.ROOT,
                "[accuracy] fields=%d valuesPerField=%d type=%s sourceBytes=%d innerHits=%d batch=%d "
                    + "estimate/hit=%.1f B actual/hit=%.1f B estimate/actual=%.3f delta/hit=%.1f B",
                fieldCount,
                valuesPerField,
                valueType,
                sourceBytes,
                innerHitCount,
                batch,
                estimatePerHit,
                actualPerHit,
                ratio,
                estimatePerHit - actualPerHit
            )
        );

        int timingCount = Math.min(batch, 256);
        timingHits = new SearchHit[timingCount];
        for (int i = 0; i < timingCount; i++) {
            timingHits[i] = buildHit(i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        timingHits = null;
    }

    @Benchmark
    public long estimateRamBytesUsed() {
        long total = 0L;
        for (SearchHit hit : timingHits) {
            total += hit.ramBytesUsed();
        }
        return total;
    }

    private int batchSize() {
        int vpf = Math.max(1, valuesPerField);
        long perHitObjects = Math.max(1L, (long) fieldCount * vpf + (long) innerHitCount * vpf);
        long derived = TARGET_FIELD_OBJECTS / perHitObjects;
        return (int) Math.max(MIN_BATCH, Math.min(MAX_BATCH, derived));
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

    private static long usedHeapAfterGc() {
        long used = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            System.gc();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            used = Math.min(used, MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed());
        }
        return used;
    }
}
