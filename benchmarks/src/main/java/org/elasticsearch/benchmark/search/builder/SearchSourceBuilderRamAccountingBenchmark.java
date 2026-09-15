/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.builder;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Justifies {@code TransportSearchAction.SEARCH_SOURCE_HEAP_OVERHEAD_FACTOR} with measured data.
 * <p>
 * The request breaker charges {@code FACTOR x getUncompressedSerializedSize(source)} to approximate the source's
 * retained on-heap footprint (the thing that actually risks OOM). This benchmark measures, per representative
 * {@link SearchSourceBuilder} shape, the true retained heap via Lucene's {@link RamUsageTester} and prints
 * {@code serialized | retained | ratio=retained/serialized}. The worst-case ratio across common (charged) shapes,
 * rounded up, is the smallest safe {@code FACTOR}: the breaker must over-count, never under-count.
 * <p>
 * The {@code serialized} column uses the exact call the breaker uses
 * ({@link DelayableWriteable#getUncompressedSerializedSize}), so the measured "serialized" matches what is charged.
 * <p>
 * Shapes cover the divergent cases where serialized is much smaller than retained (many small query-builder objects,
 * short-string {@code terms}) and the degenerate cases where the ratio approaches 1 (one large string / a dense float
 * vector — serialized bytes dominate object overhead). Degenerate shapes are safe to under-multiply and must not drive
 * {@code FACTOR}; they are printed for completeness and flagged in the {@code degenerate} column.
 * <p>
 * The ratio is deterministic (a pure function of the object graph via {@link RamUsageTester}); the JMH throughput
 * numbers are incidental. The {@code [ratio]} line printed in {@code @Setup} is the actual deliverable.
 * <p>
 * Run with:
 * <pre>
 *   ./gradlew :benchmarks:run --args="SearchSourceBuilderRamAccountingBenchmark"
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class SearchSourceBuilderRamAccountingBenchmark {

    @Param(
        {
            "terms_10",
            "terms_1k",
            "terms_100k",
            "bool_deep_wide",
            "knn_128",
            "knn_1024",
            "range_many",
            "long_utf8_string",
            "base64_string",
            "script_query",
            "script_score",
            "kitchen_sink" }
    )
    private String shape;

    private SearchSourceBuilder source;

    @Setup
    public void setup() {
        source = SearchSourceShapes.build(shape);

        long serialized = DelayableWriteable.getUncompressedSerializedSize(source);
        long retained = RamUsageTester.ramUsed(source);
        double ratio = serialized == 0L ? Double.NaN : retained / (double) serialized;
        boolean degenerate = SearchSourceShapes.isDegenerate(shape);

        System.out.printf(
            Locale.ROOT,
            "[ratio] shape=%-18s serialized=%,12d B  retained=%,14d B  ratio=%6.2f  degenerate=%b%n",
            shape,
            serialized,
            retained,
            ratio,
            degenerate
        );
    }

    @Benchmark
    public void serializedSize(Blackhole bh) {
        bh.consume(DelayableWriteable.getUncompressedSerializedSize(source));
    }

    @Benchmark
    public void retainedHeap(Blackhole bh) {
        bh.consume(RamUsageTester.ramUsed(source));
    }

    /**
     * Shared shape catalogue. Mirrored (by shape name) in {@code SearchSourceHeapOverheadTests} so the benchmark and the
     * CI regression guard measure the same graphs. Kept deterministic (no randomness) so both are reproducible.
     */
    static final class SearchSourceShapes {

        private SearchSourceShapes() {}

        /** Shapes whose serialized size is below the breaker's min-charge floor, or whose ratio degenerates to ~1. */
        static boolean isDegenerate(String shape) {
            return switch (shape) {
                case "terms_10", "knn_128", "knn_1024", "long_utf8_string", "base64_string", "script_query", "script_score" -> true;
                case "terms_1k", "terms_100k", "bool_deep_wide", "range_many", "kitchen_sink" -> false;
                default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
            };
        }

        static SearchSourceBuilder build(String shape) {
            return switch (shape) {
                case "terms_10" -> new SearchSourceBuilder().query(termsQuery(10));
                case "terms_1k" -> new SearchSourceBuilder().query(termsQuery(1_000));
                case "terms_100k" -> new SearchSourceBuilder().query(termsQuery(100_000));
                case "bool_deep_wide" -> new SearchSourceBuilder().query(boolTree());
                case "knn_128" -> new SearchSourceBuilder().knnSearch(List.of(knn(128)));
                case "knn_1024" -> new SearchSourceBuilder().knnSearch(List.of(knn(1024)));
                case "range_many" -> new SearchSourceBuilder().query(manyRanges(2_000));
                case "long_utf8_string" -> new SearchSourceBuilder().query(new TermQueryBuilder("field", utf8String(200_000)));
                case "base64_string" -> new SearchSourceBuilder().query(new TermQueryBuilder("field", base64String(200_000)));
                case "script_query" -> new SearchSourceBuilder().query(new ScriptQueryBuilder(bigScript()));
                case "script_score" -> new SearchSourceBuilder().query(
                    new ScriptScoreQueryBuilder(QueryBuilders.matchAllQuery(), bigScript())
                );
                case "kitchen_sink" -> kitchenSink();
                default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
            };
        }

        /** Fixed-length (16-char ASCII) short strings, the keyword-terms case the heap dump flagged. */
        private static String shortTerm(int i) {
            return String.format(Locale.ROOT, "term-%011d", i);
        }

        private static TermsQueryBuilder termsQuery(int count) {
            List<String> terms = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                terms.add(shortTerm(i));
            }
            return new TermsQueryBuilder("field", terms);
        }

        /** A deep, wide bool tree: each level fans out to several nested bool clauses plus term leaves. */
        private static QueryBuilder boolTree() {
            return boolTree(6, 4);
        }

        private static QueryBuilder boolTree(int depth, int fanout) {
            if (depth == 0) {
                return new TermQueryBuilder("field", shortTerm(depth));
            }
            BoolQueryBuilder bool = new BoolQueryBuilder();
            for (int i = 0; i < fanout; i++) {
                bool.must(boolTree(depth - 1, fanout));
                bool.should(new TermQueryBuilder("f" + i, shortTerm(i)));
            }
            return bool;
        }

        private static KnnSearchBuilder knn(int dims) {
            float[] vec = new float[dims];
            for (int i = 0; i < dims; i++) {
                vec[i] = i * 0.001f;
            }
            return new KnnSearchBuilder("vector", vec, 10, 100, null, null, null);
        }

        private static QueryBuilder manyRanges(int count) {
            BoolQueryBuilder bool = new BoolQueryBuilder();
            for (int i = 0; i < count; i++) {
                RangeQueryBuilder range = new RangeQueryBuilder("field_" + i);
                range.gte(i);
                range.lte(i + 1000);
                bool.filter(range);
            }
            return bool;
        }

        private static String utf8String(int chars) {
            StringBuilder sb = new StringBuilder(chars);
            for (int i = 0; i < chars; i++) {
                // CJK code point: 3 bytes in modified UTF-8, 2 bytes retained (UTF-16 compact string coder).
                sb.append((char) (0x4E00 + (i % 0x1000)));
            }
            return sb.toString();
        }

        private static String base64String(int chars) {
            StringBuilder sb = new StringBuilder(chars);
            String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            for (int i = 0; i < chars; i++) {
                sb.append(alphabet.charAt(i % alphabet.length()));
            }
            return sb.toString();
        }

        private static Script bigScript() {
            return new Script(utf8AsciiScript(4_000));
        }

        private static String utf8AsciiScript(int chars) {
            StringBuilder sb = new StringBuilder(chars);
            for (int i = 0; i < chars; i++) {
                sb.append("doc['f").append(i % 50).append("'].value + ");
            }
            sb.append('0');
            return sb.toString();
        }

        private static SearchSourceBuilder kitchenSink() {
            BoolQueryBuilder bool = new BoolQueryBuilder();
            bool.must(termsQuery(5_000));
            bool.filter(manyRanges(500));
            bool.should(boolTree(4, 3));
            bool.must(new ScriptQueryBuilder(bigScript()));
            return new SearchSourceBuilder().query(bool).knnSearch(List.of(knn(256)));
        }
    }
}
