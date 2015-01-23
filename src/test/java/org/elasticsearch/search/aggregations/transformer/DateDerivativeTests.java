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

package org.elasticsearch.search.aggregations.transformer;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.derivative;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class DateDerivativeTests extends ElasticsearchIntegrationTest {

    private DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private DateTime date(String date) {
        return DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date);
    }

    private static String format(DateTime date, String pattern) {
        return DateTimeFormat.forPattern(pattern).print(date);
    }

    private IndexRequestBuilder indexDoc(String idx, DateTime date, int value) throws Exception {
        return client().prepareIndex(idx, "type").setSource(
                jsonBuilder().startObject().field("date", date).field("value", value).startArray("dates").value(date)
                        .value(date.plusMonths(1).plusDays(1)).endArray().endObject());
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(
                jsonBuilder().startObject().field("value", value).field("date", date(month, day)).startArray("dates")
                        .value(date(month, day)).value(date(month + 1, day + 1)).endArray().endObject());
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        // TODO: would be nice to have more random data here
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field("value", i * 2).endObject()));
        }
        builders.addAll(Arrays.asList(indexDoc(1, 2, 1), // date: Jan 2, dates: Jan 2, Feb 3
                indexDoc(2, 2, 2), // date: Feb 2, dates: Feb 2, Mar 3
                indexDoc(2, 15, 3), // date: Feb 15, dates: Feb 15, Mar 16
                indexDoc(3, 2, 4), // date: Mar 2, dates: Mar 2, Apr 3
                indexDoc(3, 15, 5), // date: Mar 15, dates: Mar 15, Apr 16
                indexDoc(3, 23, 6))); // date: Mar 23, dates: Mar 23, Apr 24
        indexRandom(true, builders);
        ensureSearchable();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private static Histogram.Bucket getBucket(Derivative histogram, DateTime key) {
        return getBucket(histogram, key, DateFieldMapper.Defaults.DATE_TIME_FORMATTER.format());
    }

    private static Histogram.Bucket getBucket(Derivative histogram, DateTime key, String format) {
        return histogram.getBucketByKey("" + key.getMillis());
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        derivative("deriv").subAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(2));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = getBucket(deriv, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(0l));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(deriv, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));
    }

    @AwaitsFix(bugUrl = "Fix factory selection for serialisation of Internal derivative")
    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        derivative("deriv").subAggregation(
                                dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                                        .subAggregation(sum("sum").field("value")))).execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(2));
        Object[] propertiesKeys = (Object[]) deriv.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) deriv.getProperty("_count");
        Object[] propertiesDocCountDerivs = (Object[]) deriv.getProperty("_doc_count.value");
        Object[] propertiesCounts = (Object[]) deriv.getProperty("sum.value");

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        Histogram.Bucket bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));
        SimpleValue sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.value(), equalTo(4.0));
        assertThat((String) propertiesKeys[0], equalTo("2012-01-01T00:00:00.000Z"));
        assertThat((long) propertiesDocCounts[0], equalTo(0l));
        assertThat((double) propertiesDocCountDerivs[0], equalTo(1.0));
        assertThat((double) propertiesCounts[0], equalTo(4.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.value(), equalTo(10.0));
        assertThat((String) propertiesKeys[1], equalTo("2012-02-01T00:00:00.000Z"));
        assertThat((long) propertiesDocCounts[1], equalTo(0l));
        assertThat((double) propertiesDocCountDerivs[1], equalTo(1.0));
        assertThat((double) propertiesCounts[1], equalTo(10.0));
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        derivative("deriv").subAggregation(dateHistogram("histo").field("dates").interval(DateHistogram.Interval.MONTH)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        Histogram.Bucket bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(2.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(2.0));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-2.0));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        derivative("deriv").subAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        derivative("deriv").subAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(2));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        Histogram.Bucket bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = deriv.getBucketByKey(String.valueOf(key));
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(0l));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));
    }

}
