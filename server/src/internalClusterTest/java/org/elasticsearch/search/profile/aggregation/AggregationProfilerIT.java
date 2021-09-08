/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import io.github.nik9000.mapmatcher.MapMatcher;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedOrdinalsSamplerAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.nik9000.mapmatcher.ListMatcher.matchesList;
import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.diversifiedSampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class AggregationProfilerIT extends ESIntegTestCase {
    private static final String BUILD_LEAF_COLLECTOR = AggregationTimingType.BUILD_LEAF_COLLECTOR.toString();
    private static final String COLLECT = AggregationTimingType.COLLECT.toString();
    private static final String POST_COLLECTION = AggregationTimingType.POST_COLLECTION.toString();
    private static final String INITIALIZE = AggregationTimingType.INITIALIZE.toString();
    private static final String BUILD_AGGREGATION = AggregationTimingType.BUILD_AGGREGATION.toString();
    private static final String REDUCE = AggregationTimingType.REDUCE.toString();
    private static final Set<String> BREAKDOWN_KEYS = Set.of(
        INITIALIZE,
        BUILD_LEAF_COLLECTOR,
        COLLECT,
        POST_COLLECTION,
        BUILD_AGGREGATION,
        REDUCE,
        INITIALIZE + "_count",
        BUILD_LEAF_COLLECTOR + "_count",
        COLLECT + "_count",
        POST_COLLECTION + "_count",
        BUILD_AGGREGATION + "_count",
        REDUCE + "_count"
    );

    private static final String TOTAL_BUCKETS = "total_buckets";
    private static final String BUILT_BUCKETS = "built_buckets";
    private static final String DEFERRED = "deferred_aggregators";
    private static final String COLLECTION_STRAT = "collection_strategy";
    private static final String RESULT_STRAT = "result_strategy";
    private static final String HAS_FILTER = "has_filter";
    private static final String SEGMENTS_WITH_SINGLE = "segments_with_single_valued_ords";
    private static final String SEGMENTS_WITH_MULTI = "segments_with_multi_valued_ords";

    private static final String NUMBER_FIELD = "number";
    private static final String TAG_FIELD = "tag";
    private static final String STRING_FIELD = "string_field";

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx")
                .setSettings(Map.of("number_of_shards", 1, "number_of_replicas", 0))
                .setMapping(STRING_FIELD, "type=keyword", NUMBER_FIELD, "type=integer", TAG_FIELD, "type=keyword").get());
        List<IndexRequestBuilder> builders = new ArrayList<>();

        String[] randomStrings = new String[randomIntBetween(2, 10)];
        for (int i = 0; i < randomStrings.length; i++) {
            randomStrings[i] = randomAlphaOfLength(10);
        }

        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx").setSource(
                    jsonBuilder().startObject()
                        .field(STRING_FIELD, randomFrom(randomStrings))
                        .field(NUMBER_FIELD, randomIntBetween(0, 9))
                        .field(TAG_FIELD, randomBoolean() ? "more" : "less")
                        .endObject()));
        }

        indexRandom(true, false, builders);
        createIndex("idx_unmapped");
    }

    public void testSimpleProfile() {
        SearchResponse response = client().prepareSearch("idx").setProfile(true)
                .addAggregation(histogram("histo").field(NUMBER_FIELD).interval(1L)).get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(),
                    equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(0));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> breakdown = histoAggResult.getTimeBreakdown();
            assertThat(breakdown, notNullValue());
            assertThat(breakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(breakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(breakdown.get(COLLECT), greaterThan(0L));
            assertThat(breakdown.get(BUILD_AGGREGATION).longValue(), greaterThan(0L));
            assertThat(breakdown.get(REDUCE), equalTo(0L));
            assertMap(
                histoAggResult.getDebugInfo(),
                matchesMap().entry(TOTAL_BUCKETS, greaterThan(0L)).entry(BUILT_BUCKETS, greaterThan(0))
            );
        }
    }

    public void testMultiLevelProfile() {
        SearchResponse response = client().prepareSearch("idx").setProfile(true)
                .addAggregation(
                    histogram("histo")
                        .field(NUMBER_FIELD)
                        .interval(1L)
                        .subAggregation(
                            terms("terms")
                                .field(TAG_FIELD)
                                .order(BucketOrder.aggregation("avg", false))
                                .subAggregation(avg("avg").field(NUMBER_FIELD))
                        )
                ).get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(),
                    equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            assertMap(
                histoAggResult.getDebugInfo(),
                matchesMap().entry(TOTAL_BUCKETS, greaterThan(0L)).entry(BUILT_BUCKETS, greaterThan(0))
            );

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            assertThat(termsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(termsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(termsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(termsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(termsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(termsAggResult);
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertMap(
                avgAggResult.getDebugInfo(),
                matchesMap().entry(BUILT_BUCKETS, greaterThan(0))
            );
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    private void assertRemapTermsDebugInfo(ProfileResult termsAggResult, String... deferredAggregators) {
        MapMatcher matcher = matchesMap().entry(TOTAL_BUCKETS, greaterThan(0L))
            .entry(BUILT_BUCKETS, greaterThan(0))
            .entry(COLLECTION_STRAT, "remap using many bucket ords")
            .entry(RESULT_STRAT, "terms")
            .entry(HAS_FILTER, false)
            .entry(SEGMENTS_WITH_SINGLE, greaterThan(0))
            .entry(SEGMENTS_WITH_MULTI, 0);
        if (deferredAggregators.length > 0) {
            matcher = matcher.entry(DEFERRED, List.of(deferredAggregators));
        }
        assertMap(termsAggResult.getDebugInfo(), matcher);
    }

    public void testMultiLevelProfileBreadthFirst() {
        SearchResponse response = client().prepareSearch("idx").setProfile(true)
                .addAggregation(histogram("histo").field(NUMBER_FIELD).interval(1L).subAggregation(terms("terms")
                        .collectMode(SubAggCollectionMode.BREADTH_FIRST).field(TAG_FIELD).subAggregation(avg("avg").field(NUMBER_FIELD))))
                .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(),
                    equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            assertMap(
                histoAggResult.getDebugInfo(),
                matchesMap().entry(TOTAL_BUCKETS, greaterThan(0L)).entry(BUILT_BUCKETS, greaterThan(0))
            );
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            assertThat(termsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(termsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(termsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(termsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(termsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(termsAggResult, "avg");
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertMap(
                avgAggResult.getDebugInfo(),
                matchesMap().entry(BUILT_BUCKETS, greaterThan(0))
            );
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testDiversifiedAggProfile() {
        SearchResponse response = client().prepareSearch("idx").setProfile(true)
                .addAggregation(diversifiedSampler("diversify").shardSize(10).field(STRING_FIELD).maxDocsPerValue(2)
                        .subAggregation(max("max").field(NUMBER_FIELD)))
                .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult diversifyAggResult = aggProfileResultsList.get(0);
            assertThat(diversifyAggResult, notNullValue());
            assertThat(diversifyAggResult.getQueryName(),
                    equalTo(DiversifiedOrdinalsSamplerAggregator.class.getSimpleName()));
            assertThat(diversifyAggResult.getLuceneDescription(), equalTo("diversify"));
            assertThat(diversifyAggResult.getTime(), greaterThan(0L));
            Map<String, Long> diversifyBreakdown = diversifyAggResult.getTimeBreakdown();
            assertThat(diversifyBreakdown, notNullValue());
            assertThat(diversifyBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(diversifyBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(diversifyBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(diversifyBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(diversifyBreakdown.get(REDUCE), equalTo(0L));
            assertMap(diversifyAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)).entry(DEFERRED, List.of("max")));

            ProfileResult maxAggResult = diversifyAggResult.getProfiledChildren().get(0);
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getLuceneDescription(), equalTo("max"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(diversifyBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(diversifyBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(diversifyBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertMap(maxAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testComplexProfile() {
        SearchResponse response = client().prepareSearch("idx").setProfile(true)
                .addAggregation(histogram("histo").field(NUMBER_FIELD).interval(1L)
                        .subAggregation(terms("tags").field(TAG_FIELD)
                                .subAggregation(avg("avg").field(NUMBER_FIELD))
                                .subAggregation(max("max").field(NUMBER_FIELD)))
                        .subAggregation(terms("strings").field(STRING_FIELD)
                                .subAggregation(avg("avg").field(NUMBER_FIELD))
                                .subAggregation(max("max").field(NUMBER_FIELD))
                                .subAggregation(terms("tags").field(TAG_FIELD)
                                        .subAggregation(avg("avg").field(NUMBER_FIELD))
                                        .subAggregation(max("max").field(NUMBER_FIELD)))))
                .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(),
                    equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            assertMap(
                histoAggResult.getDebugInfo(),
                matchesMap().entry(TOTAL_BUCKETS, greaterThan(0L)).entry(BUILT_BUCKETS, greaterThan(0))
            );
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> histoAggResultSubAggregations = histoAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult tagsAggResult = histoAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            assertThat(tagsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(tagsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(tagsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(tagsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(tagsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(tagsAggResult);
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertMap(avgAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertMap(maxAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult stringsAggResult = histoAggResultSubAggregations.get("strings");
            assertThat(stringsAggResult, notNullValue());
            assertThat(stringsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(stringsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> stringsBreakdown = stringsAggResult.getTimeBreakdown();
            assertThat(stringsBreakdown, notNullValue());
            assertThat(stringsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(stringsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(stringsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(stringsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(stringsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(stringsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(stringsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(stringsAggResult);
            assertThat(stringsAggResult.getProfiledChildren().size(), equalTo(3));

            Map<String, ProfileResult> stringsAggResultSubAggregations = stringsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = stringsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertMap(avgAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = stringsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertMap(maxAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            tagsAggResult = stringsAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getLuceneDescription(), equalTo("tags"));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            assertThat(tagsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(tagsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(tagsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(tagsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(tagsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(tagsAggResult);
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertMap(avgAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertMap(maxAggResult.getDebugInfo(), matchesMap().entry(BUILT_BUCKETS, greaterThan(0)));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testNoProfile() {
        SearchResponse response = client().prepareSearch("idx").setProfile(false)
                .addAggregation(histogram("histo").field(NUMBER_FIELD).interval(1L)
                        .subAggregation(terms("tags").field(TAG_FIELD)
                                .subAggregation(avg("avg").field(NUMBER_FIELD))
                                .subAggregation(max("max").field(NUMBER_FIELD)))
                        .subAggregation(terms("strings").field(STRING_FIELD)
                                .subAggregation(avg("avg").field(NUMBER_FIELD))
                                .subAggregation(max("max").field(NUMBER_FIELD))
                                .subAggregation(terms("tags").field(TAG_FIELD)
                                        .subAggregation(avg("avg").field(NUMBER_FIELD))
                                        .subAggregation(max("max").field(NUMBER_FIELD)))))
                .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(0));
    }

    /**
     * Makes sure that when the conditions are right we run {@code date_histogram}
     * using {@code filters}. When the conditions are right, this is significantly
     * faster than the traditional execution mechanism. This is in this test
     * rather than a yaml integration test because it requires creating many many
     * documents and that is hard to express in yaml.
     */
    public void testFilterByFilter() throws InterruptedException, IOException {
        assertAcked(client().admin().indices().prepareCreate("dateidx")
            .setSettings(Map.of("number_of_shards", 1, "number_of_replicas", 0))
            .setMapping("date", "type=date").get());
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS * 2; i++) {
            String date = Instant.ofEpochSecond(i).toString();
            builders.add(client().prepareIndex("dateidx").setSource(jsonBuilder().startObject().field("date", date).endObject()));
        }
        indexRandom(true, false, builders);

        SearchResponse response = client().prepareSearch("dateidx")
            .setProfile(true)
            .addAggregation(new DateHistogramAggregationBuilder("histo").field("date").calendarInterval(DateHistogramInterval.MONTH)
                // Add a sub-agg so we don't get to use metadata. That's great and all, but it outputs less debugging info for us to verify.
                .subAggregation(new MaxAggregationBuilder("m").field("date")))
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("dateidx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(), equalTo("DateHistogramAggregator.FromDateRange"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> breakdown = histoAggResult.getTimeBreakdown();
            assertThat(breakdown, notNullValue());
            assertThat(breakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            assertThat(breakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(breakdown.get(COLLECT), equalTo(0L));
            assertThat(breakdown.get(BUILD_AGGREGATION).longValue(), greaterThan(0L));
            assertThat(breakdown.get(REDUCE), equalTo(0L));
            assertMap(
                histoAggResult.getDebugInfo(),
                matchesMap().entry(BUILT_BUCKETS, greaterThan(0))
                    .entry("delegate", "RangeAggregator.FromFilters")
                    .entry(
                        "delegate_debug",
                        matchesMap().entry("average_docs_per_range", equalTo(RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS * 2))
                            .entry("ranges", 1)
                            .entry("delegate", "FilterByFilterAggregator")
                            .entry(
                                "delegate_debug",
                                matchesMap().entry("segments_with_deleted_docs", 0)
                                    .entry("segments_with_doc_count_field", 0)
                                    .entry("segments_counted", 0)
                                    .entry("segments_collected", greaterThan(0))
                                    .entry(
                                        "filters",
                                        matchesList().item(
                                            matchesMap().entry("query", "DocValuesFieldExistsQuery [field=date]")
                                                .entry("specialized_for", "docvalues_field_exists")
                                                .entry("results_from_metadata", 0)
                                        )
                                    )
                            )
                    )
            );
        }
    }

    public void testDateHistogramFilterByFilterDisabled() throws InterruptedException, IOException {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(SearchService.ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER.getKey(), false))
        );
        try {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate("date_filter_by_filter_disabled")
                    .setSettings(Map.of("number_of_shards", 1, "number_of_replicas", 0))
                    .setMapping("date", "type=date", "keyword", "type=keyword")
                    .get()
            );
            List<IndexRequestBuilder> builders = new ArrayList<>();
            for (int i = 0; i < RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS * 2; i++) {
                String date = Instant.ofEpochSecond(i).toString();
                builders.add(
                    client().prepareIndex("date_filter_by_filter_disabled")
                        .setSource(
                            jsonBuilder().startObject()
                                .field("date", date)
                                .endObject()
                        )
                );
            }
            indexRandom(true, false, builders);

            SearchResponse response = client().prepareSearch("date_filter_by_filter_disabled")
                .setProfile(true)
                .addAggregation(new DateHistogramAggregationBuilder("histo").field("date").calendarInterval(DateHistogramInterval.MONTH))
                .get();
            assertSearchResponse(response);
            Map<String, ProfileShardResult> profileResults = response.getProfileResults();
            assertThat(profileResults, notNullValue());
            assertThat(profileResults.size(), equalTo(getNumShards("date_filter_by_filter_disabled").numPrimaries));
            for (ProfileShardResult profileShardResult : profileResults.values()) {
                assertThat(profileShardResult, notNullValue());
                AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
                assertThat(aggProfileResults, notNullValue());
                List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
                assertThat(aggProfileResultsList, notNullValue());
                assertThat(aggProfileResultsList.size(), equalTo(1));
                ProfileResult histoAggResult = aggProfileResultsList.get(0);
                assertThat(histoAggResult, notNullValue());
                assertThat(histoAggResult.getQueryName(), equalTo("DateHistogramAggregator.FromDateRange"));
                assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
                assertThat(histoAggResult.getProfiledChildren().size(), equalTo(0));
                assertThat(histoAggResult.getTime(), greaterThan(0L));
                Map<String, Long> breakdown = histoAggResult.getTimeBreakdown();
                assertMap(
                    breakdown,
                    matchesMap().entry(INITIALIZE, greaterThan(0L))
                        .entry(INITIALIZE + "_count", greaterThan(0L))
                        .entry(BUILD_LEAF_COLLECTOR, greaterThan(0L))
                        .entry(BUILD_LEAF_COLLECTOR + "_count", greaterThan(0L))
                        .entry(COLLECT, greaterThan(0L))
                        .entry(COLLECT + "_count", greaterThan(0L))
                        .entry(POST_COLLECTION, greaterThan(0L))
                        .entry(POST_COLLECTION + "_count", 1L)
                        .entry(BUILD_AGGREGATION, greaterThan(0L))
                        .entry(BUILD_AGGREGATION + "_count", greaterThan(0L))
                        .entry(REDUCE, 0L)
                        .entry(REDUCE + "_count", 0L)
                );
                Map<String, Object> debug = histoAggResult.getDebugInfo();
                assertMap(
                    debug,
                    matchesMap().entry("delegate", "RangeAggregator.NoOverlap")
                        .entry("built_buckets", 1)
                        .entry("delegate_debug", matchesMap().entry("ranges", 1).entry("average_docs_per_range", 10000.0))
                );
            }
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull(SearchService.ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER.getKey()))
            );
        }
    }
}
