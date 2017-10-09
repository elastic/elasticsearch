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

package org.elasticsearch.search.aggregations.pipeline.bucketsort;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.bucketSort;
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
    private static final String VALUE_FIELD = "value";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex(INDEX, INDEX_WITH_GAPS);
        client().admin().indices().preparePutMapping(INDEX)
                .setType("doc")
                .setSource("time", "type=date", "foo", "type=keyword", "value", "type=float")
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
                    builders.add(client().prepareIndex(INDEX, "doc")
                            .setSource(newDocBuilder(time, term, randomIntBetween(1, 10) * randomDouble())));
                }
            }
            time += TimeValue.timeValueHours(1).millis();
        }

        builders.add(client().prepareIndex(INDEX_WITH_GAPS, "doc").setSource(newDocBuilder(1, "foo", 1.0)));
        builders.add(client().prepareIndex(INDEX_WITH_GAPS, "doc").setSource(newDocBuilder(3, "foo", 3.0)));

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder(long timeMillis, String fooValue, double value) throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(TIME_FIELD, timeMillis);
        jsonBuilder.field(TERM_FIELD, fooValue);
        jsonBuilder.field(VALUE_FIELD, value);
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testEmptyBucketSort() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                    .subAggregation(bucketSort("bucketSort", Collections.emptyList())))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        long previousCount = termsBuckets.get(0).getDocCount();
        for (Terms.Bucket termBucket : termsBuckets) {
            assertThat(previousCount, greaterThanOrEqualTo(termBucket.getDocCount()));
            previousCount = termBucket.getDocCount();
        }

        response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Collections.emptyList()).size(3)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms size3Terms = response.getAggregations().get("foos");
        assertThat(size3Terms, notNullValue());
        List<? extends Terms.Bucket> size3TermsBuckets = size3Terms.getBuckets();

        for (int i = 0; i < size3TermsBuckets.size(); ++i) {
            assertThat(size3TermsBuckets.get(i), equalTo(termsBuckets.get(i)));
        }

        response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Collections.emptyList()).size(3).from(2)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms size3From2Terms = response.getAggregations().get("foos");
        assertThat(size3From2Terms, notNullValue());
        List<? extends Terms.Bucket> size3From2TermsBuckets = size3From2Terms.getBuckets();

        for (int i = 0; i < size3From2TermsBuckets.size(); ++i) {
            assertThat(size3From2TermsBuckets.get(i), equalTo(termsBuckets.get(i + 2)));
        }
    }

    public void testSortTermsOnKey() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("_key")))))
                .execute().actionGet();

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

    public void testSortTermsOnSubAggregation() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(avg("avg_value").field(VALUE_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC)))))
                .execute().actionGet();

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
                        .subAggregation(avg("avg_value").field(VALUE_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).size(2).from(3)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms size2From3Terms = response.getAggregations().get("foos");
        assertThat(size2From3Terms, notNullValue());
        List<? extends Terms.Bucket> size2From3TermsBuckets = size2From3Terms.getBuckets();
        for (int i = 0; i < size2From3TermsBuckets.size(); ++i) {
            assertThat(size2From3TermsBuckets.get(i), equalTo(termsBuckets.get(i + 3)));
        }
    }

    public void testSortTermsOnCountWithSecondarySort() {
        SearchResponse response = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(avg("avg_value").field(VALUE_FIELD))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("_count").order(SortOrder.ASC),
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC)))))
                .execute().actionGet();

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
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> ascendingTimeBuckets = histo.getBuckets();

        response = client().prepareSearch(INDEX)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("_key").order(SortOrder.DESC)))))
                .execute().actionGet();

        assertSearchResponse(response);

        histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> descendingTimeBuckets = histo.getBuckets();

        assertThat(ascendingTimeBuckets.size(), equalTo(descendingTimeBuckets.size()));
        int bucketCount = ascendingTimeBuckets.size();
        for (int i = 0; i < bucketCount; ++i) {
            assertThat(ascendingTimeBuckets.get(i), equalTo(descendingTimeBuckets.get(bucketCount - i - 1)));
        }
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsSkip() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value").field("value"))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).gapPolicy(
                                        BucketHelpers.GapPolicy.SKIP)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> timeBuckets = histo.getBuckets();
        assertThat(timeBuckets.size(), equalTo(2));
        assertThat(timeBuckets.get(0).getKey(), equalTo(3.0));
        assertThat(timeBuckets.get(1).getKey(), equalTo(1.0));
    }

    public void testSortHistogram_GivenGapsAndGapPolicyIsInsertZeros() {
        SearchResponse response = client().prepareSearch(INDEX_WITH_GAPS)
                .addAggregation(histogram("time_buckets").field(TIME_FIELD).interval(1)
                        .subAggregation(avg("avg_value").field("value"))
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(
                                new FieldSortBuilder("avg_value").order(SortOrder.DESC))).gapPolicy(
                                        BucketHelpers.GapPolicy.INSERT_ZEROS)))
                .execute().actionGet();

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
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("foos");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.isEmpty(), is(true));
    }

    public void testInvalidPath() {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_FIELD)
                        .subAggregation(bucketSort("bucketSort", Arrays.asList(new FieldSortBuilder("invalid")))))
                .execute().actionGet());
        assertThat(e.getCause().getMessage(), containsString("No aggregation found for path [invalid]"));
    }
}
