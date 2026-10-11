/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Regression tests for the {@code DocValuesRangeIterator.docIDRunEnd()} false-positive bug
 * ({@code apache/lucene#16450}, {@code elastic/elasticsearch#155653}).
 *
 * <p><b>Purpose.</b> These tests document the full surface area of the bug. Every test should usually
 * fail on commit {@code 934ab6600f64} (the last commit before the fix) and pass on commit
 * {@code 070455c8c667} (where the fix was introduced). Numeric range tests using the tsdb codec
 * will pass on the old commit because the codec has its own numeric range query.
 *
 * <p><b>Root cause.</b> {@code DenseConjunctionBulkScorer} skips per-doc {@code matches()} for any
 * clause whose {@code docIDRunEnd()} reaches the end of the current scoring window, calling
 * {@code collectRange} directly. Two {@code DocValuesRangeIterator} implementations over-reported
 * the run end:
 * <ul>
 *   <li>{@code DocValuesBlockRangeIterator} — non-contiguous ordinal sets (e.g. {@code termsQuery}
 *       with a gap between matched ordinals); returned {@code docID + 1} unconditionally.</li>
 *   <li>{@code BulkBlockRangeIterator} family — contiguous ordinal ranges and value-range queries
 *       (e.g. single-term {@code termQuery} on an {@code index=false} keyword); delegated to
 *       {@code SkipBlockRangeIterator.docIDRunEnd()} which returned {@code docID + 1}.</li>
 * </ul>
 *
 * <p><b>Triggering conditions.</b> The bug fires when a scoring window is narrow enough that the
 * over-reported {@code docIDRunEnd()} reaches its end:
 * <ul>
 *   <li><b>Segment tail:</b> The last window in a segment is often narrow, naturally satisfying
 *       the threshold.</li>
 *   <li><b>MUST_NOT boundary:</b> {@code ReqExclBulkScorer} clamps each window to just before the
 *       next excluded doc, so every MUST_NOT match creates a narrow window — the dominant failure
 *       mode in practice.</li>
 * </ul>
 *
 * <p><b>Required boolean shapes.</b>
 * <ul>
 *   <li>{@code DocValuesBlockRangeIterator} is a {@code TwoPhaseIterator}: a single FILTER does
 *       not reach {@code DenseConjunctionBulkScorer} and a second FILTER is required. With
 *       MUST_NOT: two FILTERs + MUST_NOT.</li>
 *   <li>{@code BulkBlockRangeIterator} family is a plain {@code DocIdSetIterator}: in all tested
 *       cases a single FILTER + MUST_NOT is sufficient.</li>
 * </ul>
 *
 * <p><b>ES query paths.</b>
 * <ul>
 *   <li>{@code DocValuesBlockRangeIterator}: {@code termsQuery} on an {@code index=false} keyword
 *       with non-contiguous matched ordinals; {@code regexpQuery}/{@code wildcardQuery} with
 *       {@code DOC_VALUES_REWRITE} when matched ordinals have gaps.</li>
 *   <li>{@code BulkBlockRangeIterator}: {@code termQuery} on an {@code index=false} keyword
 *       (→ {@code BulkOrdinalRangeIterator}); {@code termQuery("_routing",v)} with
 *       {@code _routing.doc_values: true} (→ {@code BulkSortedRangeIterator}, the only ES path to
 *       the single-valued variant); {@code rangeQuery} on an {@code index=false} numeric field in a
 *       standard index with {@code USE_DOC_VALUES_SKIPPER} enabled (→ {@code BulkSortedNumericRangeIterator},
 *       reached when neither the primary-sort nor TSDB-bitmask optimisation applies in
 *       {@code SortedNumericDocValuesRangeQuery}).</li>
 * </ul>
 */
public class DocValuesRangeIteratorFalsePositiveReproductionTests extends ESSingleNodeTestCase {

    private static final String INDEX = "test";
    int sortKey = 0;

    @Before
    private void setup() {
        sortKey = 0;
    }

    /**
     * Base settings for all non-TSDB reproduction tests.
     *
     * <ul>
     * <li>{@code USE_DOC_VALUES_SKIPPER = true}: required to activate the
     *     {@code DocValuesRangeIterator} code path; without it, doc-values queries fall back to
     *     per-doc iteration and never reach {@code DenseConjunctionBulkScorer}'s fast path.
     * <li>{@code USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING = randomBoolean()}: randomizes the
     *     on-disk codec across test runs to cover both the standard and TSDB formats.
     * <li>{@code index.queries.cache.enabled = false}: prevents a cached result from masking a
     *     false positive on repeated runs.
     * <li>{@code index.sort.field/order}: fixes document insertion order so doc IDs are
     *     deterministic, keeping the window geometry stable across runs.
     * </ul>
     */
    private static Settings baseSettings() {
        return Settings.builder()
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), randomBoolean())
            .put("index.queries.cache.enabled", false)
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true)
            .put("index.sort.field", "sort_key")
            .put("index.sort.order", "asc")
            .build();
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> termQuery (contiguous ordinal range, single term)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent window before the final in-range doc</li>
     *   <li><b>Iterator type:</b> {@code BulkOrdinalRangeIterator} (plain {@code DocIdSetIterator})</li>
     * </ul>
     *
     * <p>{@code termQuery("dimension","required")} on an {@code index=false} keyword with
     * {@code USE_DOC_VALUES_SKIPPER} forces the doc-values path → {@code BulkOrdinalRangeIterator}
     * (single-term contiguous ordinal range). {@code BulkOrdinalRangeIterator} is a plain
     * {@code DocIdSetIterator}, so a single FILTER + MUST_NOT reaches
     * {@code DenseConjunctionBulkScorer} without a second FILTER.
     *
     * <p>The "other/included" doc at position 2047 is a false positive without the fix: it does not
     * match {@code dimension=required} but is swept in by {@code collectRange()} when the block's
     * {@code docIDRunEnd()} satisfies {@code >= minRunEndThreshold} in the final window [2046, 2048).
     * Non-TSDB counterpart to {@code testBoolMustNotWithSingleTermQuery}.
     */
    public void testTermQueryOnIndexFalseKeyword_MustNot() throws Exception {
        makeIndex("dimension", "type=keyword,index=false", "label", "type=keyword,index=false");

        for (int i = 0; i < 2046; i++) {
            indexDoc(INDEX, "dimension", "required", "label", "excluded");
        }
        indexDoc(INDEX, "dimension", "required", "label", "included");
        // Doc 2047: dimension=other — ordinal 0, outside query range for "required".
        // Without the fix, collectRange(2046, 2048) fires in the final window and sweeps this doc
        // in as a false positive (same geometry as testRoutingFieldDocValues_MustNot).
        indexDoc(INDEX, "dimension", "other", "label", "included");

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("dimension", "required"))
            .mustNot(QueryBuilders.termQuery("label", "excluded"));

        // Correct count: 1 (doc 2046, dimension=required, label=included).
        // Bug count: 2 (doc 2047, dimension=other, falsely included via collectRange).
        assertHits(INDEX, query, 1);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + FILTER</li>
     *   <li><b>Defective query:</b> termsQuery (non-contiguous ordinal set)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> segment tail (narrow final window)</li>
     *   <li><b>Iterator type:</b> {@code DocValuesBlockRangeIterator}</li>
     * </ul>
     *
     * <p>{@code termsQuery("dimension", "apple", "cherry")} on an {@code index=false} keyword maps
     * to {@code SortedSetDocValuesField.newSlowSetQuery} → {@code DocValuesBlockRangeIterator},
     * which returns {@code blockIterator.docID() + 1} as {@code docIDRunEnd} unconditionally.
     * A second FILTER (bounding range) is required to force {@code DenseConjunctionBulkScorer}.
     *
     * <p>Doc 4096 holds "banana" (ordinal 1) — inside the bounding range [apple=0, cherry=2] but
     * not in the set {0, 2}. The final window {@code [4096, 4097)} has
     * {@code minRunEndThreshold = 4097}; both clauses return {@code docIDRunEnd = 4097} → both
     * excluded → {@code collectRange(4096, 4097)} falsely collects doc 4096. "cherry" appears at
     * doc 4095 to establish ordinal 2 in the segment so the set is genuinely non-contiguous.
     */
    public void testTermsQueryOnIndexFalseKeyword_SegmentTail() throws Exception {
        makeIndex("dimension", "type=keyword,index=false");

        // Docs 0-4094: "apple" (ordinal 0) — matching, correct hits.
        for (int i = 0; i < 4095; i++) {
            indexDoc(INDEX, "dimension", "apple");
        }
        // Doc 4095: "cherry" (ordinal 2) — matching, and establishes ordinal 2 in the segment so
        // the set {"apple","cherry"} = {0,2} is non-contiguous with a gap at banana=1.
        indexDoc(INDEX, "dimension", "cherry");
        // Doc 4096: "banana" (ordinal 1) — in bounding range [apple,cherry]=[0,2] but NOT in set
        // {apple=0, cherry=2}. DocValuesBlockRangeIterator.docIDRunEnd() = blockIterator.docID()+1
        // = 4097 = minRunEndThreshold for window [4096,4097) → false positive.
        indexDoc(INDEX, "dimension", "banana");

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            // Non-contiguous set {apple=0, cherry=2}: maps to SortedSetDocValuesField.newSlowSetQuery
            // → DocValuesBlockRangeIterator with bounding range [0,2].
            .filter(QueryBuilders.termsQuery("dimension", "apple", "cherry"))
            // Bounding range [apple, cherry] = [0, 2]: maps to SortedSetDocValuesField.newSlowRangeQuery
            // → BulkOrdinalRangeIterator. YES at doc 4096 with docIDRunEnd=4097 → excluded.
            .filter(QueryBuilders.rangeQuery("dimension").gte("apple").lte("cherry"));

        // Correct count: 4096 (docs 0-4094 "apple" + doc 4095 "cherry" ∈ {apple,cherry}).
        // Bug count: 4097 (doc 4096 "banana" falsely included via collectRange(4096,4097)).
        assertHits(INDEX, query, 4096);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> termQuery (contiguous SortedDocValues range, single term)</li>
     *   <li><b>Field type:</b> {@code _routing} (SortedDocValues via
     *       {@code SortedDocValuesField.indexedField}, {@code DocValuesSkipIndexType.RANGE})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent window before the final in-range doc</li>
     *   <li><b>Iterator type:</b> {@code BulkSortedRangeIterator} (plain {@code DocIdSetIterator})</li>
     * </ul>
     *
     * <p>{@code termQuery("_routing","required")} when {@code _routing.doc_values: true} uses
     * {@code SortedDocValuesField.newSlowExactQuery} → {@code SortedDocValuesRangeQuery} →
     * {@code BulkSortedRangeIterator}. {@code _routing} is stored via
     * {@code SortedDocValuesField.indexedField}, which sets {@code DocValuesSkipIndexType.RANGE} —
     * the prerequisite for {@code DocValuesRangeIterator} to be instantiated, and the only
     * user-queryable ES field that provides this. Doc values on {@code _routing} require explicit
     * {@code doc_values: true} in the mapping (as configured below).
     *
     * <p>Same geometry as {@link #testTermQueryOnIndexFalseKeyword_MustNot}: doc 2047
     * ({@code routing="other"}) is swept in by {@code collectRange(2046, 2048)} in the final window
     * after all MUST_NOT docs are processed.
     */
    public void testRoutingFieldDocValues_MustNot() throws Exception {
        // Enable _routing doc values to force the SortedDocValues query path.
        // Without doc_values: true, _routing uses an inverted index in standard mode and
        // termQuery("_routing","value") goes through TermQuery (not DocValuesRangeIterator).
        client().admin().indices().prepareCreate(INDEX).setSettings(baseSettings()).setMapping("""
            {
              "_routing": {"doc_values": true},
              "properties": {
                "label": {"type": "keyword", "index": false},
                "sort_key": {"type": "integer"}
              }
            }""").get();

        for (int i = 0; i < 2046; i++) {
            prepareIndex(INDEX).setRouting("required")
                .setSource(XContentFactory.jsonBuilder().startObject().field("label", "excluded").field("sort_key", sortKey++).endObject())
                .get();
        }
        prepareIndex(INDEX).setRouting("required")
            .setSource(XContentFactory.jsonBuilder().startObject().field("label", "included").field("sort_key", sortKey++).endObject())
            .get();
        // Doc 2047: routing="other" — ordinal 0, outside query range [1,1] for "required".
        // Block [0, 2048) is MAYBE (docs 0–2046 in range, this doc not). Without the fix,
        // collectRange(2046, 2048) sweeps this doc into the result set as a false positive.
        prepareIndex(INDEX).setRouting("other")
            .setSource(XContentFactory.jsonBuilder().startObject().field("label", "included").field("sort_key", sortKey++).endObject())
            .get();

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("_routing", "required"))
            .mustNot(QueryBuilders.termQuery("label", "excluded"));

        // Correct count: 1 (doc 2046, routing=required, label=included).
        // Bug count: 2 (doc 2047, routing=other, falsely included via collectRange).
        assertHits(INDEX, query, 1);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> rangeQuery (contiguous numeric range, MAYBE block)</li>
     *   <li><b>Field type:</b> numeric long (SortedNumericDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent 1-doc window</li>
     *   <li><b>Iterator type:</b> {@code BulkSortedNumericRangeIterator} (plain {@code DocIdSetIterator})</li>
     * </ul>
     *
     * <p>{@code rangeQuery("num",1,10)} on an {@code index=false} numeric field with
     * {@code USE_DOC_VALUES_SKIPPER} forces the doc-values path via
     * {@code SortedNumericDocValuesField.newSlowRangeQuery()} →
     * {@code BulkSortedNumericRangeIterator}. A single FILTER + MUST_NOT suffices because
     * {@code BulkSortedNumericRangeIterator} is a plain {@code DocIdSetIterator}.
     *
     * <p>Doc 0 has {@code num=99} (outside range [1,10]) and shares a skip block with docs 1+
     * having {@code num=5} (inside range) — a MAYBE block. In 10.5.0,
     * {@code BulkBlockRangeIterator.docIDRunEnd()} delegates unconditionally to
     * {@code SkipBlockRangeIterator.docIDRunEnd()}. {@code ReqExclBulkScorer} creates window
     * {@code [0,1)} before the first MUST_NOT at doc 1: {@code minRunEndThreshold = min(0+2048, 1) = 1},
     * {@code docIDRunEnd() >= 1} → clause excluded → {@code collectRange(0,1)} fires → doc 0
     * (num=99) is a false positive.
     */
    public void testNumericRange_MaybeBlock_MustNot() throws Exception {
        makeIndex("num", "type=long,index=false", "flag", "type=keyword");

        // Doc 0: num=99 (outside range [1,10]) — false positive candidate.
        // Shares skip block with docs 1+ (all num=5, in range), making the block MAYBE.
        // Without the fix, collectRange(0, 1) fires in the 1-doc window before the first MUST_NOT.
        indexDoc(INDEX, "num", "99", "flag", "included");
        // Docs 1-2046: MUST_NOT. The first excluded doc at position 1 creates the critical
        // 1-doc window [0, 1) that triggers the bug.
        for (int i = 1; i < 2047; i++) {
            indexDoc(INDEX, "num", "5", "flag", "excluded");
        }
        // Doc 2047: num=5 (in range [1,10]), flag=included — the single correct hit.
        indexDoc(INDEX, "num", "5", "flag", "included");

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("num").from(1L).to(10L))
            .mustNot(QueryBuilders.termQuery("flag", "excluded"));

        // Correct count: 1 (doc 2047, num=5, flag=included).
        // Bug count: 2 (doc 0, num=99, falsely included via collectRange).
        assertHits(INDEX, query, 1);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> rangeQuery (contiguous numeric range, YES_IF_PRESENT block)</li>
     *   <li><b>Field type:</b> numeric long (SortedNumericDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent 1-doc window</li>
     *   <li><b>Iterator type:</b> {@code BulkSortedNumericRangeIterator} (plain {@code DocIdSetIterator})</li>
     * </ul>
     *
     * <p>Same code path as {@link #testNumericRange_MaybeBlock_MustNot} but the false-positive doc
     * has no value for {@code num} rather than an out-of-range value. All other present values are
     * {@code num=5 ∈ [1,10]}, so Lucene classifies the block as YES_IF_PRESENT (all present values
     * in range, one doc missing).
     *
     * <p>The no-value doc must be at position 1, not 0: {@code SkipBlockRangeIterator.advance(0)}
     * skips to the first doc in the block that has a value, so a no-value doc at position 0 would
     * be bypassed entirely. Placing an excluded (MUST_NOT) doc at position 0 first causes
     * {@code ReqExclBulkScorer} to set {@code upTo=1}, after which the 1-doc window {@code [1,2)}
     * lands correctly on the no-value doc.
     */
    public void testNumericRange_SparseField_MustNot() throws Exception {
        makeIndex("num", "type=long,index=false", "flag", "type=keyword");

        // Doc 0: num=5 (in range [1,10]), flag=excluded — first MUST_NOT.
        // ReqExclBulkScorer skips doc 0 and sets upTo=1.
        indexDoc(INDEX, "num", "5", "flag", "excluded");
        // Doc 1: no value for num — false positive candidate.
        // SkipBlockRangeIterator.advance(1) slow-path: nextDoc = max(1, skipper.minDocID(0)=0) = 1.
        // YES_IF_PRESENT block: docIDRunEnd() = 2 = minRunEndThreshold → collectRange(1,2) fires.
        indexDoc(INDEX, "flag", "included");
        // Docs 2-2046: MUST_NOT. The first excluded doc after the gap at position 2 creates
        // the critical 1-doc window [1, 2) that triggers the bug.
        for (int i = 2; i < 2047; i++) {
            indexDoc(INDEX, "num", "5", "flag", "excluded");
        }
        // Doc 2047: num=5 (in range [1,10]), flag=included — the single correct hit.
        indexDoc(INDEX, "num", "5", "flag", "included");

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("num").from(1L).to(10L))
            .mustNot(QueryBuilders.termQuery("flag", "excluded"));

        // Correct count: 1 (doc 2047, num=5, flag=included).
        // Bug count: 2 (doc 1, no value for num, falsely included via collectRange).
        assertHits(INDEX, query, 1);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + FILTER</li>
     *   <li><b>Defective query:</b> termsQuery (non-contiguous ordinal set — gap between matched ordinals)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> segment tail (last doc in a narrow window)</li>
     *   <li><b>Iterator type:</b> {@code DocValuesBlockRangeIterator} ({@code TwoPhaseIterator})</li>
     * </ul>
     *
     * <p>{@code termsQuery("value","aaa","ccc")} maps to the non-contiguous ordinal set {@code {0,2}}
     * (gap at ord 1, "bbb"), using {@code DocValuesBlockRangeIterator} whose buggy
     * {@code docIDRunEnd()} returns {@code docID + 1} unconditionally. Because
     * {@code DocValuesBlockRangeIterator} is a {@code TwoPhaseIterator},
     * {@code ConstantScoreScorerSupplier} falls back for a single FILTER; a second FILTER is
     * required to reach {@code DenseConjunctionBulkScorer.of()}.
     *
     * <p>With 4097 docs the last doc ("bbb") falls in a 1-doc tail window; the second filter covers
     * the full ordinal range with YES blocks; both clauses are excluded from {@code windowClauses};
     * {@code collectRange} fires and "bbb" is returned as a false positive.
     */
    public void testTermsQueryNonContiguous_TwoFilters_SegmentTail() throws Exception {
        makeIndex("value", "type=keyword,index=false");

        // DenseConjunctionBulkScorer.WINDOW_SIZE + 1 so the final doc falls in a second window.
        int maxDoc = 4097;
        for (int i = 0; i < maxDoc; i++) {
            // Last doc gets "bbb" (ord 1): inside bounding range [0,2] but not in the set {0,2}.
            String val = (i == maxDoc - 1) ? "bbb" : (i % 100 == 0 ? "ccc" : "aaa");
            indexDoc(INDEX, "value", val);
        }

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("value", "aaa", "ccc"))
            .filter(QueryBuilders.regexpQuery("value", ".*")); // match all

        // Correct count: 4096 ("aaa" and "ccc" docs at positions 0–4095).
        // Bug count: 4097 (doc 4096 "bbb" falsely included via collectRange).
        assertHits(INDEX, query, maxDoc - 1);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> termsQuery (non-contiguous ordinal set — gap between matched ordinals)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent 1-doc window</li>
     *   <li><b>Iterator type:</b> {@code DocValuesBlockRangeIterator} ({@code TwoPhaseIterator})</li>
     * </ul>
     *
     * <p>Same non-contiguous ordinal set as
     * {@link #testTermsQueryNonContiguous_TwoFilters_SegmentTail} —
     * {@code termsQuery("value","aaa","ccc")} → {@code DocValuesBlockRangeIterator} — but triggered
     * via MUST_NOT. A second FILTER covering the contiguous range [aaa,bbb,ccc] is still required
     * because {@code DocValuesBlockRangeIterator} is a {@code TwoPhaseIterator}.
     *
     * <p>Doc 0 has value "bbb": inside bounding range [0,2] but not in the set {aaa,ccc}. Doc 1 is
     * the first MUST_NOT, creating window {@code [0,1)}. Both clauses satisfy
     * {@code docIDRunEnd() >= 1 = minRunEndThreshold}; {@code collectRange(0,1)} fires; doc 0 is a
     * false positive. Requires {@code maxDoc >= 4096}.
     */
    public void testTermsQueryNonContiguous_TwoFilters_MustNot() throws Exception {
        makeIndex("value", "type=keyword,index=false", "flag", "type=keyword");

        // Doc 0: "bbb/include" — in bounding range [0,2] but not in set {aaa,ccc}. False positive.
        indexDoc(INDEX, "value", "bbb", "flag", "include");
        // Doc 1: first MUST_NOT — terminates the 1-doc window [0, 1).
        indexDoc(INDEX, "value", "aaa", "flag", "excluded");
        // Docs 2-4096: mix of "aaa" and "ccc" with flag="include" (4095 expected hits).
        // "ccc" docs ensure ordinal 2 exists, making the set {0,2} genuinely non-contiguous.
        // 4097 total docs needed: DenseConjunctionBulkScorer.of() requires maxDoc >= 4096.
        for (int i = 2; i < 4097; i++) {
            indexDoc(INDEX, "value", (i % 100 == 0 ? "ccc" : "aaa"), "flag", "include");
        }

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        // Correct count: 4095 ("aaa" and "ccc" docs at positions 2-4096).
        // Bug count: 4096 (doc 0 "bbb" falsely included via collectRange).
        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("value", "aaa", "ccc"))          // non-contiguous {0,2} → DocValuesBlockRangeIterator
            .filter(QueryBuilders.termsQuery("value", "aaa", "bbb", "ccc"))   // contiguous [0,2] → BulkOrdinalRangeIterator YES blocks
            .mustNot(QueryBuilders.termQuery("flag", "excluded"));
        assertHits(INDEX, query, 4095);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + FILTER</li>
     *   <li><b>Defective query:</b> regexpQuery with {@code DOC_VALUES_REWRITE} (non-contiguous match set)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> segment tail (last doc in a narrow window)</li>
     *   <li><b>Iterator type:</b> {@code DocValuesBlockRangeIterator} ({@code TwoPhaseIterator})</li>
     * </ul>
     *
     * <p>{@code regexpQuery("value","aaa|ccc")} with {@code DOC_VALUES_REWRITE} matches ordinals
     * {@code {0,2}} — non-contiguous (gap at ord 1, "bbb") — creating a
     * {@code DocValuesBlockRangeIterator}. Because it is a {@code TwoPhaseIterator}, a second FILTER
     * is required to reach {@code DenseConjunctionBulkScorer.of()}.
     *
     * <p>Doc 4096 has value "bbb" (ord 1): inside the bounding range [0,2] but not matched by the
     * regexp. It falls in a 1-doc tail window; the second filter covers the whole range with YES
     * blocks; both clauses are excluded from {@code windowClauses}; {@code collectRange} fires.
     */
    public void testRegexpDocValuesNonContiguous_TwoFilters_SegmentTail() throws Exception {
        makeIndex("value", "type=keyword,index=false");

        // 4097 docs: mostly "aaa" (ord 0), some "ccc" (ord 2) to create the non-contiguous set.
        // Last doc is "bbb" (ord 1): inside bounding range [0,2] but not matched by the regexp.
        for (int i = 0; i < 4097; i++) {
            String val = (i == 4096) ? "bbb" : (i % 100 == 0 ? "ccc" : "aaa");
            indexDoc(INDEX, "value", val);
        }

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        // Correct count: 4096 ("aaa" and "ccc" docs; "bbb" doc rejected by filter 1 (regexp aaa|ccc)).
        // Bug count: 4097 ("bbb" doc at position 4096 falsely included via collectRange).
        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.regexpQuery("value", "aaa|ccc"))       // non-contiguous {0,2}
            .filter(QueryBuilders.regexpQuery("value", ".*")); // match all

        assertHits(INDEX, query, 4096);
    }

    /**
     * <ul>
     *   <li><b>Boolean shape:</b> FILTER + FILTER + MUST_NOT</li>
     *   <li><b>Defective query:</b> regexpQuery with {@code DOC_VALUES_REWRITE} (non-contiguous match set)</li>
     *   <li><b>Field type:</b> multi-valued keyword (SortedSetDocValues, {@code index=false})</li>
     *   <li><b>Trigger condition:</b> MUST_NOT-adjacent 1-doc window</li>
     *   <li><b>Iterator type:</b> {@code DocValuesBlockRangeIterator} ({@code TwoPhaseIterator})</li>
     * </ul>
     *
     * <p>Same query as {@link #testRegexpDocValuesNonContiguous_TwoFilters_SegmentTail} —
     * {@code regexpQuery("value","aaa|ccc")} → {@code DocValuesBlockRangeIterator} — but triggered
     * via MUST_NOT rather than segment tail. A second FILTER ({@code termsQuery} covering the
     * contiguous range [aaa,bbb,ccc]) is still required because {@code DocValuesBlockRangeIterator}
     * is a {@code TwoPhaseIterator}.
     *
     * <p>Doc 0 has value "bbb": matched by the contiguous FILTER 2 but not by the regexp FILTER 1.
     * Doc 1 is the first MUST_NOT, creating a 1-doc window {@code [0,1)}. Both clauses satisfy
     * {@code docIDRunEnd() >= 1 = minRunEndThreshold}, are excluded from per-doc evaluation, and
     * {@code collectRange(0,1)} fires, returning doc 0 as a false positive. Requires
     * {@code maxDoc >= 4096}.
     */
    public void testRegexpDocValuesNonContiguous_TwoFilters_MustNot() throws Exception {
        makeIndex("value", "type=keyword,index=false", "flag", "type=keyword");

        // Doc 0: "bbb/include" — in bounding range [0,2] but not matched by regexp "aaa|ccc".
        indexDoc(INDEX, "value", "bbb", "flag", "include");
        // Doc 1: first MUST_NOT — terminates the 1-doc window [0, 1).
        indexDoc(INDEX, "value", "aaa", "flag", "excluded");
        // Docs 2-4096: mix of "aaa" and "ccc" with flag="include" (4095 expected hits).
        for (int i = 2; i < 4097; i++) {
            indexDoc(INDEX, "value", (i % 100 == 0 ? "ccc" : "aaa"), "flag", "include");
        }

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        // Correct count: 4095 ("aaa" and "ccc" docs at positions 2-4096).
        // Bug count: 4096 (doc 0 "bbb" falsely included via collectRange).
        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.regexpQuery("value", "aaa|ccc"))              // non-contiguous {0,2} → DocValuesBlockRangeIterator
            .filter(QueryBuilders.termsQuery("value", "aaa", "bbb", "ccc"))    // contiguous [0,2] → BulkOrdinalRangeIterator YES blocks
            .mustNot(QueryBuilders.termQuery("flag", "excluded"));
        assertHits(INDEX, query, 4095);
    }

    ////////////////////////////////// Legacy Tests Below //////////////////////////////////

    /**
     * This is a legacy reproduction test. The above queries should cover all cases, but these
     * are left in just to be extra careful.
     * Exercises term queries bug: {@code termQuery} on a TSDB dimension field is executed as
     * a doc-values ordinal range lookup. Indexes 2048 docs so the mixed {@code dimension} block has
     * {@code MAYBE} status. 2046 docs have {@code dimension=required, label=excluded},
     * one has {@code dimension=required, label=included} (expected hit), and one has
     * {@code dimension=other, label=included} (false positive without the fix).
     */
    public void testBoolMustNotWithSingleTermQuery() throws Exception {
        final String index = "tsdb-bool-repro";

        createIndex(
            index,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
                .put("index.queries.cache.enabled", false)
                .build(),
            "@timestamp",
            "type=date",
            "dimension",
            "type=keyword,time_series_dimension=true",
            "label",
            "type=keyword,index=false"
        );

        final long baseTs = 1704067200000L; // 2024-01-01T00:00:00Z
        for (int i = 0; i < 2046; i++) {
            prepareIndex(index).setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", baseTs + i)
                    .field("dimension", "required")
                    .field("label", "excluded")
                    .endObject()
            ).get();
        }
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", baseTs + 2046)
                .field("dimension", "required")
                .field("label", "included")
                .endObject()
        ).get();
        // This doc must not appear in results (filter: dimension=required does not match).
        // Without the fix, docIDRunEnd() for the YES_IF_PRESENT mixed block returns the block end,
        // so the bulk scorer skips matches() and collects this doc as a false positive.
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", baseTs + 2047)
                .field("dimension", "other")
                .field("label", "included")
                .endObject()
        ).get();

        client().admin().indices().prepareRefresh(index).get();
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("dimension", "required"))
            .mustNot(QueryBuilders.termQuery("label", "excluded"));

        assertHits(index, query, 1);
    }

    /**
     * This is a legacy reproduction test. The above queries should cover all cases, but these
     * are left in just to be extra careful.
     * Exercises a {@code termsQuery} filter alongside a {@code termQuery} filter and
     * {@code mustNot} on a force-merged single segment. Also checks that {@code profile:true} and
     * {@code profile:false} agree — the original symptom was that profiling bypassed the bulk
     * scorer and returned correct results while the normal path returned false positives.
     */
    public void testBoolMustNotWithTermsQuery() throws Exception {
        final String index = "tsdb-bool-two-filter-repro";

        createIndex(
            index,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim_a")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
                .put("index.queries.cache.enabled", false)
                .build(),
            "@timestamp",
            "type=date",
            "dim_a",
            "type=keyword,time_series_dimension=true",
            "dim_b",
            "type=keyword,time_series_dimension=true",
            "tag",
            "type=keyword,index=false"
        );

        long ts = 1704067200000L; // 2024-01-01T00:00:00Z

        // wrong dim_a — excluded by first filter
        for (int i = 0; i < 3000; i++) {
            indexDoc(index, ts++, "other", "match", "alpha");
        }
        // right dim_a + dim_b, but tag matches mustNot — large block that triggers the bug
        for (int i = 0; i < 2500; i++) {
            indexDoc(index, ts++, "target", "match", "excluded");
        }
        // wrong dim_b — excluded by second filter
        for (int i = 0; i < 80; i++) {
            indexDoc(index, ts++, "target", "mismatch", "beta");
        }
        // all conditions satisfied — expected hits
        for (int i = 0; i < 100; i++) {
            indexDoc(index, ts++, "target", "match", "gamma");
        }

        client().admin().indices().prepareRefresh(index).get();
        // Single segment ensures DenseConjunctionBulkScorer is reliably triggered.
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("dim_a", "target"))
            .filter(QueryBuilders.termsQuery("dim_b", "match"))
            .mustNot(QueryBuilders.termQuery("tag", "excluded"));

        assertHits(index, query, 100);
    }

    private void indexDoc(String index, long timestampMs, String dimA, String dimB, String tag) throws Exception {
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", timestampMs)
                .field("dim_a", dimA)
                .field("dim_b", dimB)
                .field("tag", tag)
                .endObject()
        ).get();
    }

    private void indexDoc(String index, String... fieldAndValues) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < fieldAndValues.length; i += 2) {
            var field = fieldAndValues[i];
            var value = fieldAndValues[i + 1];
            xContentBuilder.field(field, value);
        }
        xContentBuilder.field("sort_key", sortKey++);
        xContentBuilder.endObject();
        prepareIndex(index).setSource(xContentBuilder).get();
    }

    private void makeIndex(String... fields) {
        String[] withSortKey = new String[fields.length + 2];
        System.arraycopy(fields, 0, withSortKey, 0, fields.length);
        withSortKey[fields.length] = "sort_key";
        withSortKey[fields.length + 1] = "type=integer";
        createIndex(INDEX, baseSettings(), withSortKey);
    }

    private void assertHits(String index, QueryBuilder query, int expectedHits) {
        // assert results of query
        assertHitCount(client().prepareSearch(index).setTrackTotalHits(true).setQuery(query), expectedHits);

        // also get query results with profiling and assert that it's the same
        long[] profileHits = new long[1];
        assertResponse(
            client().prepareSearch(index).setTrackTotalHits(true).setProfile(true).setQuery(query),
            resp -> profileHits[0] = resp.getHits().getTotalHits().value()
        );

        assertEquals("profile:true should return the correct count", expectedHits, profileHits[0]);
    }
}
