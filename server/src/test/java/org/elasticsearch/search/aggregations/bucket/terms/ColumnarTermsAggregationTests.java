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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A {@code terms} aggregation over a columnar keyword field has to give what an ordinary keyword field gives for the same
 * documents. The aggregation reads the column's dictionary ordinals and hashes each term once for each segment, so the
 * documents here have few distinct terms, some terms that escape the dictionary, and some documents with no value.
 */
public class ColumnarTermsAggregationTests extends ESSingleNodeTestCase {

    private static final String COLUMNAR = "columnar-index";
    private static final String PLAIN = "plain-index";

    private static final String[] COMMON_VALUES = { "alpha", "bravo", "charlie", "delta", "echo" };

    @Before
    public void indexTheSameDocumentsTwice() {
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
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR).build();
    }

    /**
     * Mostly one value from a few, and some documents with none. One in twelve has a long value that occurs once. There
     * are too many of those for the column's dictionary to name them all, so some probably escape it, which is not
     * checked here.
     */
    private static Map<String, Object> source(int doc) {
        final Map<String, Object> source = new TreeMap<>();
        source.put("@timestamp", "2024-01-01T00:00:00Z");
        source.put("n", doc % 7);
        // Some documents stand for several, which the buckets have to add up.
        source.put("_doc_count", 1 + doc % 4);
        if (doc % 20 == 0) {
            return source; // no kw
        }
        source.put("kw", doc % 12 == 1 ? "unique-" + doc + "-" + "x".repeat(120) : COMMON_VALUES[doc % COMMON_VALUES.length]);
        return source;
    }

    public void testTermsMatchesAnOrdinaryKeywordField() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = buckets(PLAIN, terms, false, false);
        assertThat(expected.size(), greaterThan(COMMON_VALUES.length));
        assertThat(buckets(COLUMNAR, terms, false, false), equalTo(expected));
    }

    /** A sub-aggregation still sees every document, and it adds up. */
    public void testASubAggregationStillAddsUp() {
        final AggregationBuilder terms = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .subAggregation(AggregationBuilders.sum("s").field("n"));
        assertThat(buckets(COLUMNAR, terms, true, false), equalTo(buckets(PLAIN, terms, true, false)));
    }

    /** With hits tracked the aggregation counts the same. */
    public void testWithHitsTrackedCountsTheSame() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(COLUMNAR, terms, false, true), equalTo(buckets(PLAIN, terms, false, true)));
    }

    /** Terms that are excluded are left out, whether they are in the dictionary or escaped. */
    public void testIncludeAndExcludeCountTheSame() {
        final AggregationBuilder excluded = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .includeExclude(new IncludeExclude(null, "alpha|bravo", null, null));
        final Map<String, Long> expected = buckets(PLAIN, excluded, false, false);
        assertFalse(expected.containsKey("alpha"));
        assertThat(buckets(COLUMNAR, excluded, false, false), equalTo(expected));

        final AggregationBuilder included = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .includeExclude(new IncludeExclude("echo", null, null, null));
        assertThat(buckets(COLUMNAR, included, false, false), equalTo(buckets(PLAIN, included, false, false)));
    }

    /** A document that stands for several, through {@code _doc_count}, counts for all of them. */
    public void testDocCountFieldIsAddedUp() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = buckets(PLAIN, terms, false, false);
        final long[] documents = new long[1];
        assertResponse(
            client().prepareSearch(PLAIN).setSize(0).setTrackTotalHits(true),
            response -> documents[0] = response.getHits().getTotalHits().value()
        );
        assertThat(
            "some documents stand for several",
            expected.values().stream().mapToLong(Long::longValue).sum(),
            greaterThan(documents[0])
        );
        assertThat(buckets(COLUMNAR, terms, false, false), equalTo(expected));
    }

    /** {@code significant_terms} counts the foreground and the background of each term. */
    public void testSignificantTermsCountTheSame() {
        final AggregationBuilder significant = AggregationBuilders.significantTerms("s").field("kw").size(10_000).minDocCount(1);
        final Consumer<SearchRequestBuilder> foreground = request -> request.setQuery(QueryBuilders.termQuery("n", 3));
        final Map<String, Long> expected = counts(PLAIN, foreground, significant);
        assertThat(expected.size(), greaterThan(COMMON_VALUES.length));
        assertThat(counts(COLUMNAR, foreground, significant), equalTo(expected));
    }

    /** Several aggregations at the top share one collector, and each still counts every document. */
    public void testTwoTopLevelAggregationsCountTheSame() {
        final AggregationBuilder first = AggregationBuilders.terms("first").field("kw").size(10_000);
        final AggregationBuilder second = AggregationBuilders.terms("second").field("kw").size(10_000);
        final Map<String, Long> expected = counts(PLAIN, request -> {}, first, second);
        assertThat(counts(COLUMNAR, request -> {}, first, second), equalTo(expected));
    }

    /** A profiled search wraps the collectors, and counts the same. */
    public void testProfiledSearchCountsTheSame() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Consumer<SearchRequestBuilder> profile = request -> request.setProfile(true);
        assertThat(counts(COLUMNAR, profile, terms), equalTo(counts(PLAIN, profile, terms)));
    }

    /** {@code min_score} and {@code terminate_after} wrap the collectors, and with neither cutting any off the counts are the same. */
    public void testMinScoreAndTerminateAfterCountTheSame() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Consumer<SearchRequestBuilder> wrapped = request -> request.setMinScore(0.5f).setTerminateAfter(1_000_000);
        final Map<String, Long> expected = counts(PLAIN, wrapped, terms);
        assertThat(expected.size(), greaterThan(COMMON_VALUES.length));
        assertThat(counts(COLUMNAR, wrapped, terms), equalTo(expected));
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

    /** A field with several values a document counts the same, whichever way each document is read. */
    public void testAMultiValuedFieldCountsTheSame() {
        final String columnar = "columnar-multi";
        final String plain = "plain-multi";
        createIndex(columnar, columnarSettings(), "@timestamp", "type=date", "kw", "type=keyword");
        createIndex(plain, Settings.EMPTY, "@timestamp", "type=date", "kw", "type=keyword");
        final BulkRequestBuilder columnarBulk = client().prepareBulk();
        final BulkRequestBuilder plainBulk = client().prepareBulk();
        for (int doc = 0; doc < 3000; doc++) {
            final String value = COMMON_VALUES[doc % COMMON_VALUES.length];
            final Map<String, Object> source = new TreeMap<>();
            source.put("@timestamp", "2024-01-01T00:00:00Z");
            source.put("kw", doc % 3 == 0 ? List.of(value, value) : List.of(value, COMMON_VALUES[(doc + 1) % COMMON_VALUES.length]));
            columnarBulk.add(prepareIndex(columnar).setSource(source));
            plainBulk.add(prepareIndex(plain).setSource(source));
        }
        assertFalse(columnarBulk.get().hasFailures());
        assertFalse(plainBulk.get().hasFailures());
        indicesAdmin().prepareRefresh(columnar, plain).get();
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(buckets(columnar, terms, false, false), equalTo(buckets(plain, terms, false, false)));
    }

    /** Under another aggregation, terms is collected for many owning buckets, and counts the same. */
    public void testANestedAggregationCountsTheSame() {
        final AggregationBuilder outer = AggregationBuilders.terms("outer")
            .field("n")
            .size(100)
            .subAggregation(AggregationBuilders.terms("t").field("kw").size(10_000));
        final Map<String, Long> expected = nested(PLAIN, outer);
        assertThat(expected.size(), greaterThan(7));
        assertThat(nested(COLUMNAR, outer), equalTo(expected));
    }

    /** Outer key and inner key to doc count. */
    private Map<String, Long> nested(String index, AggregationBuilder outer) {
        final Map<String, Long> buckets = new TreeMap<>();
        final SearchRequestBuilder request = client().prepareSearch(index).setSize(0).setTrackTotalHits(false).addAggregation(outer);
        assertNoFailuresAndResponse(request, response -> {
            final LongTerms outerTerms = response.getAggregations().get("outer");
            for (LongTerms.Bucket bucket : outerTerms.getBuckets()) {
                final StringTerms inner = bucket.getAggregations().get("t");
                for (StringTerms.Bucket innerBucket : inner.getBuckets()) {
                    buckets.put(bucket.getKeyAsString() + "/" + innerBucket.getKeyAsString(), innerBucket.getDocCount());
                }
            }
        });
        return buckets;
    }

    /**
     * Aggregation name and key to doc count, for terms and significant terms alike. Hits are not tracked, and a
     * significant term has its subset and superset counts too.
     */
    private Map<String, Long> counts(String index, Consumer<SearchRequestBuilder> customize, AggregationBuilder... aggregations) {
        final Map<String, Long> counts = new TreeMap<>();
        final SearchRequestBuilder request = client().prepareSearch(index).setSize(0).setTrackTotalHits(false);
        for (AggregationBuilder aggregation : aggregations) {
            request.addAggregation(aggregation);
        }
        customize.accept(request);
        assertNoFailuresAndResponse(request, response -> {
            for (AggregationBuilder aggregation : aggregations) {
                final MultiBucketsAggregation result = response.getAggregations().get(aggregation.getName());
                for (MultiBucketsAggregation.Bucket bucket : result.getBuckets()) {
                    final String key = aggregation.getName() + "/" + bucket.getKeyAsString();
                    counts.put(key, bucket.getDocCount());
                    if (bucket instanceof SignificantTerms.Bucket significant) {
                        counts.put(key + "#subsetDf", significant.getSubsetDf());
                        counts.put(key + "#supersetDf", significant.getSupersetDf());
                    }
                }
            }
        });
        return counts;
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
