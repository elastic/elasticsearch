/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Regression test for the empty {@code collectRange} bug in {@code RangeBulkScorer}
 * ({@code apache/lucene#16546}).
 *
 * <p><b>Root cause.</b> When a {@code RangeBulkScorer} (used for index-sorted doc-values exact
 * queries) is wrapped by {@code ReqExclBulkScorer}, an empty scoring window can be passed with
 * {@code min == max}. {@code RangeBulkScorer} unconditionally calls
 * {@code LeafCollector.collectRange(filteredMin, filteredMax)}, but the default implementation
 * rejects empty ranges via {@code RangeDocIdStream}.
 *
 * <p><b>Elasticsearch trigger path.</b>
 * <ul>
 *   <li>{@code index.sort.field} matches the filtered field so {@code termQuery} on an
 *       {@code index=false} keyword maps to {@code RangeBulkScorer} via
 *       {@link org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery}.</li>
 *   <li>{@code index.mapping.use_doc_values_skipper=true} is required to write the doc-values
 *       skip index that enables the bulk scorer fast path.</li>
 *   <li>A boolean query with a dense FILTER and a high-exclusion MUST_NOT drives
 *       {@code ReqExclBulkScorer} to create empty scoring windows.</li>
 *   <li>Aggregations (and other collectors inheriting the default {@code collectRange}) surface
 *       the bug as a search failure: {@code IllegalArgumentException: min = 4096 >= max = 4096}.</li>
 * </ul>
 */
public class RangeBulkScorerEmptyRangeReproductionTests extends ESSingleNodeTestCase {

    private static final String INDEX = "range-bulk-scorer-empty-range-repro";
    private static final String SLICE_FIELD = "slice";

    /** Number of matching docs: one per 819 docs in the dense {@code slice=src} block. */
    private static final int EXPECTED_HITS = 10;

    @Before
    public void setupIndex() throws IOException {
        client().admin().indices().prepareCreate(INDEX).setSettings(baseSettings()).setMapping("""
            {
              "properties": {
                "slice": {"type": "keyword", "index": false},
                "excluded": {"type": "keyword"}
              }
            }
            """).get();

        for (int i = 0; i < 128; i++) {
            indexDoc("aaa", "no");
        }
        for (int i = 0; i < 8192; i++) {
            indexDoc("src", i % 819 == 818 ? "no" : "yes");
        }
        for (int i = 0; i < 128; i++) {
            indexDoc("zzz", "no");
        }

        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
    }

    /**
     * Reproduces the bug through the full Elasticsearch search path with a terms aggregation.
     * {@code LeafBucketCollector} inherits the default {@code collectRange} implementation.
     */
    public void testBoolMustNotWithTermsAggregation() {
        assertResponse(
            client().prepareSearch(INDEX)
                .setTrackTotalHits(true)
                .setSize(0)
                .setQuery(query())
                .addAggregation(AggregationBuilders.terms("by_excluded").field("excluded")),
            response -> {
                assertEquals(EXPECTED_HITS, response.getHits().getTotalHits().value());
                Terms terms = response.getAggregations().get("by_excluded");
                assertNotNull(terms);
                assertEquals(1, terms.getBuckets().size());
                assertEquals("no", terms.getBuckets().getFirst().getKeyAsString());
                assertEquals(EXPECTED_HITS, terms.getBuckets().getFirst().getDocCount());
            }
        );
    }

    /**
     * Same query shape without aggregations. Also exercises the bulk scorer fast path during
     * hit counting.
     */
    public void testBoolMustNotHitCount() {
        assertHitCount(client().prepareSearch(INDEX).setTrackTotalHits(true).setQuery(query()), EXPECTED_HITS);
    }

    private static QueryBuilder query() {
        return QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(SLICE_FIELD, "src"))
            .mustNot(QueryBuilders.termQuery("excluded", "yes"));
    }

    private static Settings baseSettings() {
        return Settings.builder()
            .put("index.queries.cache.enabled", false)
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true)
            .put("index.sort.field", SLICE_FIELD)
            .put("index.sort.order", "asc")
            .build();
    }

    private void indexDoc(String slice, String excluded) throws IOException {
        prepareIndex(INDEX).setSource(
            XContentFactory.jsonBuilder().startObject().field(SLICE_FIELD, slice).field("excluded", excluded).endObject()
        ).get();
    }
}
