/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedOrdinalsSamplerAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.test.ESIntegTestCase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.diversifiedSampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class AggregationProfilerIT extends ESIntegTestCase {


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

        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
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
            assertThat(breakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(breakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(breakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(breakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(breakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(breakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(breakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(breakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));

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
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));
        }
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
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
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
            Map<String, Long> histoBreakdown = diversifyAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(diversifyAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult maxAggResult = diversifyAggResult.getProfiledChildren().get(0);
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getLuceneDescription(), equalTo("max"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(termsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
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
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(histoBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> histoAggResultSubAggregations = histoAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult tagsAggResult = histoAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> maxBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult stringsAggResult = histoAggResultSubAggregations.get("strings");
            assertThat(stringsAggResult, notNullValue());
            assertThat(stringsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(stringsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> stringsBreakdown = stringsAggResult.getTimeBreakdown();
            assertThat(stringsBreakdown, notNullValue());
            assertThat(stringsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(stringsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(stringsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(stringsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(stringsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(stringsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(stringsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(stringsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(stringsAggResult.getProfiledChildren().size(), equalTo(3));

            Map<String, ProfileResult> stringsAggResultSubAggregations = stringsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = stringsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = stringsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = stringsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = stringsAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            tagsAggResult = stringsAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getLuceneDescription(), equalTo("tags"));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(tagsBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(tagsBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren().stream()
                    .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(avgBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.INITIALIZE.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.COLLECT.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.BUILD_AGGREGATION.toString()), greaterThan(0L));
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), notNullValue());
            assertThat(maxBreakdown.get(AggregationTimingType.REDUCE.toString()), equalTo(0L));
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
}
