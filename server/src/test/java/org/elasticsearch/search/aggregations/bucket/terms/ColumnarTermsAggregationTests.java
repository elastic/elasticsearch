/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * A {@code terms} aggregation over a columnar keyword field reads the field a page at a time when it can, and has to
 * give what it gives over an ordinary keyword field holding the same documents. The same documents go into both
 * indices, so the ordinary index is the answer the columnar one is held to.
 */
public class ColumnarTermsAggregationTests extends ESSingleNodeTestCase {

    private static final String COLUMNAR = "columnar-index";
    private static final String PLAIN = "plain-index";
    private static final String PAGES_DEBUG_KEY = "columnar_page_docs";

    private static final String[] VERSIONS = { "7.17.0", "8.4.1", "8.11.0", "8.12.2", "9.0.0" };

    @Before
    public void indexTheSameDocumentsTwice() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        createIndex(
            COLUMNAR,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR)
                .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
                .build(),
            "@timestamp",
            "type=date",
            "kw",
            "type=keyword",
            "n",
            "type=long"
        );
        createIndex(PLAIN, Settings.EMPTY, "@timestamp", "type=date", "kw", "type=keyword", "n", "type=long");
        // Several refreshes, so the field is read from more than one segment and the pages of one are counted
        // before the next begins.
        final int batches = randomIntBetween(2, 4);
        int doc = 0;
        for (int batch = 0; batch < batches; batch++) {
            final int size = randomIntBetween(3000, 6000);
            final BulkRequestBuilder columnar = client().prepareBulk();
            final BulkRequestBuilder plain = client().prepareBulk();
            for (int i = 0; i < size; i++, doc++) {
                final Map<String, Object> source = source(doc);
                columnar.add(prepareIndex(COLUMNAR).setSource(source));
                plain.add(prepareIndex(PLAIN).setSource(source));
            }
            assertFalse(columnar.get().hasFailures());
            assertFalse(plain.get().hasFailures());
            indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        }
    }

    /** Mostly one value from a few, some documents with none, some with several, some with a value twice. */
    private static Map<String, Object> source(int doc) {
        final Map<String, Object> source = new TreeMap<>();
        source.put("@timestamp", "2024-01-01T00:00:00Z");
        source.put("n", doc % 7);
        final int shape = doc % 20;
        if (shape == 0) {
            return source; // no kw
        }
        if (shape == 1) {
            source.put("kw", List.of(VERSIONS[doc % VERSIONS.length], VERSIONS[(doc + 1) % VERSIONS.length]));
        } else if (shape == 2) {
            final String version = VERSIONS[doc % VERSIONS.length];
            source.put("kw", List.of(version, version));
        } else if (shape == 3) {
            source.put("kw", "rare-" + doc); // a value of its own, which a dictionary does not name
        } else {
            source.put("kw", VERSIONS[doc % VERSIONS.length]);
        }
        return source;
    }

    public void testTermsMatchesAnOrdinaryKeywordField() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = buckets(PLAIN, terms, false);
        assertThat(expected.size(), greaterThan(VERSIONS.length));
        assertThat(buckets(COLUMNAR, terms, false), equalTo(expected));
    }

    public void testTheColumnIsReadAsPages() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final List<Map<String, Object>> debug = debugInfo(COLUMNAR, terms);
        assertThat(debug.size(), equalTo(1));
        assertThat(debug.get(0), hasKey(PAGES_DEBUG_KEY));
        assertThat((Long) debug.get(0).get(PAGES_DEBUG_KEY), greaterThan(0L));
    }

    public void testTopTermsWithASmallSize() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(3);
        assertThat(buckets(COLUMNAR, terms, false), equalTo(buckets(PLAIN, terms, false)));
    }

    /** A sub-aggregation needs each document, so the field is read a document at a time and says so. */
    public void testASubAggregationIsNotCountedAsPages() {
        final AggregationBuilder terms = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .subAggregation(AggregationBuilders.sum("s").field("n"));
        assertThat(buckets(COLUMNAR, terms, true), equalTo(buckets(PLAIN, terms, true)));
        final List<Map<String, Object>> debug = debugInfo(COLUMNAR, terms);
        assertThat(debug.get(0), not(hasKey(PAGES_DEBUG_KEY)));
    }

    /** Key to doc count, and with {@code withSum} the sub-aggregation's sum too, so a lost document shows. */
    private Map<String, Long> buckets(String index, AggregationBuilder terms, boolean withSum) {
        final Map<String, Long> buckets = new TreeMap<>();
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(terms), response -> {
            final StringTerms result = response.getAggregations().get("t");
            for (StringTerms.Bucket bucket : result.getBuckets()) {
                buckets.put(bucket.getKeyAsString(), bucket.getDocCount());
                if (withSum) {
                    final Sum sum = bucket.getAggregations().get("s");
                    buckets.put(bucket.getKeyAsString() + "#sum", (long) sum.value());
                }
            }
        });
        return buckets;
    }

    /** What each aggregation reported about how it collected, from a profiled search. */
    private List<Map<String, Object>> debugInfo(String index, AggregationBuilder terms) {
        final List<Map<String, Object>> debug = new ArrayList<>();
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).setProfile(true).addAggregation(terms), response -> {
            for (SearchProfileShardResult shard : response.getSearchProfileShardResults().values()) {
                for (ProfileResult result : shard.getAggregationProfileResults().getProfileResults()) {
                    debug.add(result.getDebugInfo());
                }
            }
        });
        return debug;
    }
}
