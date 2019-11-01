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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.bucketSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketSortIT extends ESIntegTestCase {

    private static final String INDEX = "bucket-sort-it-data-index";
    private static final String INDEX_WITH_GAPS = "bucket-sort-it-data-index-with-gaps";

    private static final String TIME_FIELD = "time";
    private static final String TERM_FIELD = "foo";
    private static final String VALUE_1_FIELD = "value_1";
    private static final String VALUE_2_FIELD = "value_2";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex(INDEX, INDEX_WITH_GAPS);
        client().admin().indices().preparePutMapping(INDEX)
                .setSource("time", "type=date", "foo", "type=keyword", "value_1", "type=float", "value_2", "type=float")
                .get();

        int numTerms = 10;
        List<String> terms = new ArrayList<>(numTerms);
        for (int i = 0; i < numTerms; ++i) {
            terms.add(randomAlphaOfLengthBetween(3, 8));
        }

        long now = System.currentTimeMillis();
        long time = now - TimeValue.timeValueHours(24).millis();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        while (time < now) {
            for (String term : terms) {
                int termCount = randomIntBetween(3, 6);
                for (int i = 0; i < termCount; ++i) {
                    builders.add(client().prepareIndex(INDEX)
                            .setSource(newDocBuilder(time, term, randomIntBetween(1, 10) * randomDouble())));
                }
            }
            time += TimeValue.timeValueHours(1).millis();
        }

        builders.add(client().prepareIndex(INDEX_WITH_GAPS).setSource(newDocBuilder(1, "foo", 1.0, 42.0)));
        builders.add(client().prepareIndex(INDEX_WITH_GAPS).setSource(newDocBuilder(2, "foo", null, 42.0)));
        builders.add(client().prepareIndex(INDEX_WITH_GAPS).setSource(newDocBuilder(3, "foo", 3.0, 42.0)));

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder(long timeMillis, String fooValue, Double value1) throws IOException {
        return newDocBuilder(timeMillis, fooValue, value1, null);
    }

    private XContentBuilder newDocBuilder(long timeMillis, String fooValue, Double value1, Double value2) throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(TIME_FIELD, timeMillis);
        jsonBuilder.field(TERM_FIELD, fooValue);
        if (value1 != null) {
            jsonBuilder.field(VALUE_1_FIELD, value1);
        }
        if (value2 != null) {
            jsonBuilder.field(VALUE_2_FIELD, value2);
        }
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testEmptyBucketSort() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis()))
                .get();

        assertSearchResponse(response);

        Histogram histogram = response.getAggregations().get("time_buckets");
        assertThat(histogram, notNullValue());
        // These become our baseline
        List<? extends Histogram.Bucket> timeBuckets = histogram.getBuckets();
        ZonedDateTime previousKey = (ZonedDateTime) timeBuckets.get(0).getKey();
        for (Histogram.Bucket timeBucket : timeBuckets) {
            assertThat(previousKey, lessThanOrEqualTo((ZonedDateTime) timeBucket.getKey()));
            previousKey = (ZonedDateTime) timeBucket.getKey();
        }

        // Now let's test using size
        response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
                        .subAggregation(bucketSort("bucketSort", Collections.emptyList()).size(3)))
                .get();

        assertSearchResponse(response);

        Histogram size3Histogram = response.getAggregations().get("time_buckets");
        assertThat(size3Histogram, notNullValue());
        List<? extends Histogram.Bucket> size3TimeBuckets = size3Histogram.getBuckets();

        for (int i = 0; i < size3TimeBuckets.size(); ++i) {
            assertThat(size3TimeBuckets.get(i).getKey(), equalTo(timeBuckets.get(i).getKey()));
        }

        // Finally, let's test using size + from
        response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
                        .subAggregation(bucketSort("bucketSort", Collections.emptyList()).size(3).from(2)))
                .get();

        assertSearchResponse(response);

        Histogram size3From2Histogram = response.getAggregations().get("time_buckets");
        assertThat(size3From2Histogram, notNullValue());
        List<? extends Histogram.Bucket> size3From2TimeBuckets = size3From2Histogram.getBuckets();

        for (int i = 0; i < size3From2TimeBuckets.size(); ++i) {
            assertThat(size3From2TimeBuckets.get(i).getKey(), equalTo(timeBuckets.get(i + 2).getKey()));
        }
    }

    public void testSortTermsOnKey() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("_key")))))
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        String previousKey = (String) termsBuckets.get(0).getKey();
        for (Terms.Bucket termBucket : termsBuckets) {
            assertThat(previousKey, lessThanOrEqualTo((String) termBucket.getKey()));
            previousKey = (String) termBucket.getKey();
        }
    }

    public void testSortTermsOnKeyWithSize() {
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .addAggregation(terms("foos").field(TERM_FIELD)
                .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("_key"))).size(3)))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertEquals(3, termsBuckets.size());
        String previousKey = (String) termsBuckets.get(0).getKey();
        for (Terms.Bucket termBucket : termsBuckets) {
            assertThat(previousKey, lessThanOrEqualTo((String) termBucket.getKey()));
            previousKey = (String) termBucket.getKey();
        }
    }

    public void testSortTermsOnSubAggregation() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC)))))
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        double previousAvgValue = ((Avg) termsBuckets.get(0).getAggregations().get("avg_value")).getValue();
        for (Terms.Bucket termBucket : termsBuckets) {
            Avg avg = termBucket.getAggregations().get("avg_value");
            assertThat(avg, notNullValue());
            assertThat(previousAvgValue, greaterThanOrEqualTo(avg.getValue()));
            previousAvgValue = avg.getValue();
        }

        response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).size(2).from(3)))
                .get();

        assertSearchResponse(response);

        Terms size2From3Terms = response.getAggregations().get("foos");
        assertThat(size2From3Terms, notNullValue());
        List<? extends Terms.Bucket> size2From3TermsBuckets = size2From3Terms.getBuckets();
        for (int i = 0; i < size2From3TermsBuckets.size(); ++i) {
            assertThat(size2From3TermsBuckets.get(i).getKey(), equalTo(termsBuckets.get(i + 3).getKey()));
        }
    }

    public void testSortTermsOnSubAggregationPreservesOrderOnEquals() {
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .addAggregation(terms("foos").field(TERM_FIELD)
                .subAggregation(bucketSort("keyBucketSort", Arrays.asList(new FieldSortBuilder("_key"))))
                .subAggregation(max("max").field("missingValue").missing(1))
                .subAggregation(bucketSort("maxBucketSort", Arrays.asList(new FieldSortBuilder("max")))))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();

        // Since all max values are equal, we expect the order of keyBucketSort to have been preserved
        String previousKey = (String) termsBuckets.get(0).getKey();
        for (Terms.Bucket termBucket : termsBuckets) {
            assertThat(previousKey, lessThanOrEqualTo((String) termBucket.getKey()));
            previousKey = (String) termBucket.getKey();
        }
    }

    public void testSortTermsOnCountWithSecondarySort() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("_count").order(SortOrder.ASC),
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC)))))
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        long previousCount = termsBuckets.get(0).getDocCount();
        double previousAvgValue = ((Avg) termsBuckets.get(0).getAggregations().get("avg_value")).getValue();
        for (Terms.Bucket termBucket : termsBuckets) {
            Avg avg = termBucket.getAggregations().get("avg_value");
            assertThat(avg, notNullValue());
            assertThat(previousCount, lessThanOrEqualTo(termBucket.getDocCount()));
            if (previousCount == termBucket.getDocCount()) {
                assertThat(previousAvgValue, greaterThanOrEqualTo(avg.getValue()));
            }
            previousCount = termBucket.getDocCount();
            previousAvgValue = avg.getValue();
        }
    }

    public void testSortDateHistogramDescending() {
        SearchResponse response = client().prepareSearch(INDEX)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis()))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> ascendingTimeBuckets = histo.getBuckets();

        response = client().prepareSearch(INDEX)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("_key").order(SortOrder.DESC)))))
                .get();

        assertSearchResponse(response);

        histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> descendingTimeBuckets = histo.getBuckets();

        assertThat(ascendingTimeBuckets.size(), equalTo(descendingTimeBuckets.size()));
        int bucketCount = ascendingTimeBuckets.size();
        for (int i = 0; i < bucketCount; ++i) {
            assertThat(ascendingTimeBuckets.get(i).getKey(), equalTo(descendingTimeBuckets.get(bucketCount - i - 1).getKey()));
        }
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsSkip() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).gapPolicy(
                                        BucketHelpers.GapPolicy.SKIP)))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(2));
        assertThat(timeBuckets.get(0).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(1.0));
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsSkipAndSizeIsLessThanAvailableBuckets() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).gapPolicy(
                                        BucketHelpers.GapPolicy.SKIP).size(2)))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(2));
        assertThat(timeBuckets.get(0).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(1.0));
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsSkipAndPrimarySortHasGaps() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value_1").field(VALUE_1_FIELD))
                        .subAggregation(avg("avg_value_2").field(VALUE_2_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value_1").order(SortOrder.DESC),
                                new FieldSortBuilder("avg_value_2").order(SortOrder.DESC))).gapPolicy(
                                BucketHelpers.GapPolicy.SKIP)))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(3));
        assertThat(timeBuckets.get(0).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(1.0));
        assertThat(timeBuckets.get(2).getKey(), equalTo(2.0));
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsSkipAndSecondarySortHasGaps() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value_1").field(VALUE_1_FIELD))
                        .subAggregation(avg("avg_value_2").field(VALUE_2_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value_2").order(SortOrder.DESC),
                                new FieldSortBuilder("avg_value_1").order(SortOrder.ASC))).gapPolicy(
                                BucketHelpers.GapPolicy.SKIP)))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(3));
        assertThat(timeBuckets.get(0).getKey(), equalTo(1.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(2).getKey(), equalTo(2.0));
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsInsertZeros() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value").field(VALUE_1_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).gapPolicy(
                                        BucketHelpers.GapPolicy.INSERT_ZEROS)))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(3));
        assertThat(timeBuckets.get(0).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(1.0));
        assertThat(timeBuckets.get(2).getKey(), equalTo(2.0));
    }

    public void testEmptyBuckets() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .setQuery(QueryBuilders.existsQuery("non-field"))
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("_key")))))
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.isEmpty(), is(true));
    }

    public void testInvalidPath() {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch(INDEX)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("invalid")))))
                .get());
        assertThat(e.getCause().getMessage(), containsString("No aggregation found for path [invalid]"));
    }

    public void testNeitherSortsNorSizeSpecifiedAndFromIsDefault_ShouldThrowValidation() {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch(INDEX)
                        .addAggregation(terms("foos").field(TERM_FIELD)
                                .subAggregation(bucketSort("bucketSort", Collections.emptyList())))
                        .get());
        assertThat(e.getCause().getMessage(), containsString("[bucketSort] is configured to perform nothing." +
                " Please set either of [sort, size, from] to use bucket_sort"));
    }
}
