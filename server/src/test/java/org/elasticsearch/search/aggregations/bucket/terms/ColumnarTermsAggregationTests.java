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
 * A {@code terms} aggregation over a columnar keyword field has to give what it gives over an ordinary keyword field
 * holding the same documents, however the query phase hands the documents to it. The same documents go into both
 * indices, so the ordinary index is the answer the columnar one is held to.
 *
 * <p>The query phase gives the aggregation streams of documents only when nothing else is collecting them, which is
 * when hits are not tracked. A search that tracks hits has a collector counting them until it reaches its limit, which
 * is wrapped around the aggregation until then and hands it one document at a time, and these indices are smaller than
 * the limit. So most tests do not track hits, and one does. Profiling cannot be used to see which way the documents
 * came, because it replaces the bulk scorers that produce streams.
 */
public class ColumnarTermsAggregationTests extends ESSingleNodeTestCase {

    private static final String COLUMNAR = "columnar-index";
    private static final String PLAIN = "plain-index";

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
        // Several refreshes, so the field is read from more than one segment.
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
        final Map<String, Long> expected = buckets(PLAIN, terms, false, false);
        assertThat(expected.size(), greaterThan(VERSIONS.length));
        assertThat(buckets(COLUMNAR, terms, false, false), equalTo(expected));
    }

    /** A sub-aggregation needs each document, so the field is read a document at a time and still adds up. */
    public void testASubAggregationStillAddsUp() {
        final AggregationBuilder terms = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .subAggregation(AggregationBuilders.sum("s").field("n"));
        assertThat(buckets(COLUMNAR, terms, true, false), equalTo(buckets(PLAIN, terms, true, false)));
    }

    /** With hits tracked the aggregation is handed documents one at a time, and has to count the same. */
    public void testWithHitsTrackedCountsTheSame() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(COLUMNAR, terms, false, true), equalTo(buckets(PLAIN, terms, false, true)));
    }

    /**
     * With documents deleted, Lucene hands the aggregation windows of matches found in a bit set rather than a range of
     * documents. The counts have to leave the deleted documents out.
     */
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

    /**
     * Key to doc count, and with {@code withSum} the sub-aggregation's sum too, so a lost document shows. Hits are
     * tracked only when asked.
     */
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
