/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Regression guard proving that {@link TransportSearchAction#SEARCH_SOURCE_HEAP_OVERHEAD_FACTOR} keeps the request-breaker
 * charge ({@code FACTOR x} serialized size) a conservative upper bound on the source's true retained heap. For each
 * representative {@link SearchSourceBuilder} shape it asserts
 * {@code FACTOR * getUncompressedSerializedSize(source) >= RamUsageTester.ramUsed(source)} — i.e. the breaker over-counts,
 * never under-counts. The factor itself is derived from the same shapes by
 * {@code SearchSourceBuilderRamAccountingBenchmark}; this test is what fails if someone lowers the factor below what the
 * measured object graphs need.
 * <p>
 * Only shapes that would actually be charged drive the assertion: shapes serializing below
 * {@link TransportSearchAction#SEARCH_SOURCE_MIN_CHARGE_BYTES} are never charged in production, so their ratio is
 * irrelevant to breaker safety no matter how high it climbs (a small terms list has the largest ratio of all — fixed
 * query-builder overhead dominates its tiny serialized form — yet is never charged). Those below-floor shapes are checked
 * separately by {@link #testBelowFloorShapesAreNotCharged}, which is also why the factor is chosen from the worst
 * <em>charged</em> ratio rather than the worst ratio overall.
 * <p>
 * Shapes are deterministic (no randomness) so the measured serialized/retained bytes are reproducible.
 * {@link RamUsageTester} is an approximation of retained heap and depends on the running JVM's object layout (it is
 * measured live on the test JVM, so the comparison is self-consistent); a very large heap that disables compressed oops
 * would retain more per object than the compressed-oops layout these numbers assume.
 */
public class SearchSourceHeapOverheadTests extends ESTestCase {

    private static final double FACTOR = TransportSearchAction.SEARCH_SOURCE_HEAP_OVERHEAD_FACTOR;

    public void testChargeIsConservativeUpperBoundForChargedShapes() {
        // Every shape here serializes above the min-charge floor, so the breaker charges it in production and the charge
        // must be a conservative (>=) upper bound on the true retained heap. script_query/script_score are charged but
        // low-ratio (serialized bytes dominate); they pass with wide margin and strengthen the guard.
        for (String shape : List.of(
            "terms_1k",
            "terms_100k",
            "bool_deep_wide",
            "range_many",
            "script_query",
            "script_score",
            "kitchen_sink"
        )) {
            SearchSourceBuilder source = build(shape);
            long serialized = DelayableWriteable.getUncompressedSerializedSize(source);
            long retained = RamUsageTester.ramUsed(source);

            assertThat(
                "shape [" + shape + "] must serialize above the min-charge floor to be a charged shape",
                serialized,
                greaterThanOrEqualTo(TransportSearchAction.SEARCH_SOURCE_MIN_CHARGE_BYTES)
            );
            long charge = Math.round(FACTOR * serialized);
            assertThat(underCountMessage(shape, serialized, retained, charge), charge, greaterThanOrEqualTo(retained));
        }
    }

    /**
     * Below-floor shapes are never charged, so their retained/serialized ratio does not need to be bounded by
     * {@code FACTOR} — {@code terms_10} in fact has the highest ratio of any shape (fixed query-builder overhead swamps its
     * tiny serialized form) yet is safe precisely because it is never charged. This is why {@code FACTOR} is derived from
     * the worst <em>charged</em> ratio, not the worst ratio overall.
     */
    public void testBelowFloorShapesAreNotCharged() {
        for (String shape : List.of("terms_10", "knn_128", "knn_1024")) {
            SearchSourceBuilder source = build(shape);
            long serialized = DelayableWriteable.getUncompressedSerializedSize(source);
            assertThat(
                "shape [" + shape + "] is expected to be below the charge floor",
                serialized,
                lessThanOrEqualTo(TransportSearchAction.SEARCH_SOURCE_MIN_CHARGE_BYTES)
            );
        }
    }

    private static String underCountMessage(String shape, long serialized, long retained, long charge) {
        return String.format(
            Locale.ROOT,
            "shape [%s] under-counts retained heap: charge=%d (FACTOR=%.1f x serialized=%d) < retained=%d",
            shape,
            charge,
            FACTOR,
            serialized,
            retained
        );
    }

    // --- shape catalogue, mirrored (by name) from SearchSourceBuilderRamAccountingBenchmark ---

    private static SearchSourceBuilder build(String shape) {
        return switch (shape) {
            case "terms_10" -> new SearchSourceBuilder().query(termsQuery(10));
            case "terms_1k" -> new SearchSourceBuilder().query(termsQuery(1_000));
            case "terms_100k" -> new SearchSourceBuilder().query(termsQuery(100_000));
            case "bool_deep_wide" -> new SearchSourceBuilder().query(boolTree(6, 4));
            case "knn_128" -> new SearchSourceBuilder().knnSearch(List.of(knn(128)));
            case "knn_1024" -> new SearchSourceBuilder().knnSearch(List.of(knn(1024)));
            case "range_many" -> new SearchSourceBuilder().query(manyRanges(2_000));
            case "script_query" -> new SearchSourceBuilder().query(new ScriptQueryBuilder(bigScript()));
            case "script_score" -> new SearchSourceBuilder().query(new ScriptScoreQueryBuilder(QueryBuilders.matchAllQuery(), bigScript()));
            case "kitchen_sink" -> kitchenSink();
            default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
        };
    }

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

    private static Script bigScript() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4_000; i++) {
            sb.append("doc['f").append(i % 50).append("'].value + ");
        }
        sb.append('0');
        return new Script(sb.toString());
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
