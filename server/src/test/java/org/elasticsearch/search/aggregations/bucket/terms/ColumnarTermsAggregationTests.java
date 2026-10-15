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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A {@code terms} aggregation over a columnar keyword field has to give what an ordinary keyword field gives for the same
 * documents. Hits are not tracked in most tests, because a search that tracks them wraps the aggregation with a hit counter
 * that passes it documents one at a time until its limit. Profiling cannot show which way documents arrive, since it
 * replaces the bulk scorers.
 */
public class ColumnarTermsAggregationTests extends ESSingleNodeTestCase {

    private static final String COLUMNAR = "columnar-index";
    private static final String PLAIN = "plain-index";

    private static final String[] VERSIONS = { "7.17.0", "8.4.1", "8.11.0", "8.12.2", "9.0.0" };

    @Before
    public void indexTheSameDocumentsTwice() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        createIndex(COLUMNAR, columnarSettings(), "@timestamp", "type=date", "kw", "type=keyword", "n", "type=long");
        createIndex(PLAIN, Settings.EMPTY, "@timestamp", "type=date", "kw", "type=keyword", "n", "type=long");
        // Several refreshes, for more than one segment.
        final int batches = randomIntBetween(2, 4);
        int doc = 0;
        for (int batch = 0; batch < batches; batch++) {
            final int size = randomIntBetween(3000, 6000);
            final BulkRequestBuilder columnar = client().prepareBulk();
            final BulkRequestBuilder plain = client().prepareBulk();
            for (int i = 0; i < size; i++, doc++) {
                final Map<String, Object> source = source(doc);
                columnar.add(prepareIndex(COLUMNAR).setId(Integer.toString(doc)).setSource(source));
                plain.add(prepareIndex(PLAIN).setId(Integer.toString(doc)).setSource(source));
            }
            assertFalse(columnar.get().hasFailures());
            assertFalse(plain.get().hasFailures());
            indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        }
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    /** One value from a few, some documents with none, some with a value of their own. */
    private static Map<String, Object> source(int doc) {
        final Map<String, Object> source = new TreeMap<>();
        source.put("@timestamp", "2024-01-01T00:00:00Z");
        source.put("n", doc % 7);
        final int shape = doc % 20;
        if (shape == 0) {
            return source; // no kw
        }
        if (shape == 1) {
            source.put("kw", "rare-" + doc); // a value of its own, which a dictionary does not name
        } else {
            source.put("kw", VERSIONS[doc % VERSIONS.length]);
        }
        return source;
    }

    public void testTermsMatchesAnOrdinaryKeywordField() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = buckets(PLAIN, terms, false, false);
        assertThat(expected.size(), greaterThan(VERSIONS.length));
        assertThat(buckets(COLUMNAR, terms, false, false), equalTo(expected));
    }

    /** A sub-aggregation needs each document, so there are no pages, and it still adds up. */
    public void testASubAggregationStillAddsUp() {
        final AggregationBuilder terms = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .subAggregation(AggregationBuilders.sum("s").field("n"));
        assertThat(buckets(COLUMNAR, terms, true, false), equalTo(buckets(PLAIN, terms, true, false)));
    }

    /** With hits tracked the aggregation gets documents one at a time. */
    public void testWithHitsTrackedCountsTheSame() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(COLUMNAR, terms, false, true), equalTo(buckets(PLAIN, terms, false, true)));
    }

    /** Deleted documents are left out. */
    public void testDeletedDocumentsAreNotCounted() {
        final BulkRequestBuilder deletes = client().prepareBulk();
        final BulkRequestBuilder plainDeletes = client().prepareBulk();
        for (int doc = 0; doc < 2000; doc += randomIntBetween(1, 40)) {
            deletes.add(client().prepareDelete(COLUMNAR, Integer.toString(doc)));
            plainDeletes.add(client().prepareDelete(PLAIN, Integer.toString(doc)));
        }
        assertFalse(deletes.get().hasFailures());
        assertFalse(plainDeletes.get().hasFailures());
        indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(COLUMNAR, terms, false, false), equalTo(buckets(PLAIN, terms, false, false)));
    }

    /** A field with several values a document is not read as pages, and counts the same. */
    public void testAMultiValuedFieldCountsTheSame() {
        final String columnar = "columnar-multi";
        final String plain = "plain-multi";
        createIndex(columnar, columnarSettings(), "@timestamp", "type=date", "kw", "type=keyword");
        createIndex(plain, Settings.EMPTY, "@timestamp", "type=date", "kw", "type=keyword");
        final BulkRequestBuilder columnarBulk = client().prepareBulk();
        final BulkRequestBuilder plainBulk = client().prepareBulk();
        for (int doc = 0; doc < 3000; doc++) {
            final String version = VERSIONS[doc % VERSIONS.length];
            final Map<String, Object> source = new TreeMap<>();
            source.put("@timestamp", "2024-01-01T00:00:00Z");
            source.put("kw", doc % 3 == 0 ? List.of(version, version) : List.of(version, VERSIONS[(doc + 1) % VERSIONS.length]));
            columnarBulk.add(prepareIndex(columnar).setSource(source));
            plainBulk.add(prepareIndex(plain).setSource(source));
        }
        assertFalse(columnarBulk.get().hasFailures());
        assertFalse(plainBulk.get().hasFailures());
        indicesAdmin().prepareRefresh(columnar, plain).get();
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(columnar, terms, false, false), equalTo(buckets(plain, terms, false, false)));
    }

    /** Key to doc count, and with {@code withSum} the sub-aggregation's sum too. Hits are tracked only when asked. */
    private Map<String, Long> buckets(String index, AggregationBuilder terms, boolean withSum, boolean trackHits) {
        final Map<String, Long> buckets = new TreeMap<>();
        final SearchRequestBuilder request = client().prepareSearch(index).setSize(0).addAggregation(terms);
        if (trackHits == false) {
            request.setTrackTotalHits(false);
        }
        assertNoFailuresAndResponse(request, response -> {
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
}
