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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ESIntegTestCase.SuiteScopeTestCase
public class DateHistogramIT extends ESIntegTestCase {

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
        return client().prepareIndex(idx, "type").setSource(jsonBuilder()
                .startObject()
                .field("date", date)
                .field("value", value)
                .startArray("dates").value(date).value(date.plusMonths(1).plusDays(1)).endArray()
                .endObject());
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "_timestamp", "enabled=true"));
        createIndex("idx_unmapped");
        // TODO: would be nice to have more random data here
        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer"));
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i * 2)
                    .endObject()));
        }
        builders.addAll(Arrays.asList(
                indexDoc(1, 2, 1),  // date: Jan 2, dates: Jan 2, Feb 3
                indexDoc(2, 2, 2),  // date: Feb 2, dates: Feb 2, Mar 3
                indexDoc(2, 15, 3), // date: Feb 15, dates: Feb 15, Mar 16
                indexDoc(3, 2, 4),  // date: Mar 2, dates: Mar 2, Apr 3
                indexDoc(3, 15, 5), // date: Mar 15, dates: Mar 15, Apr 16
                indexDoc(3, 23, 6))); // date: Mar 23, dates: Mar 23, Apr 24
        indexRandom(true, builders);
        ensureSearchable();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private static String getBucketKeyAsString(DateTime key) {
        return getBucketKeyAsString(key, DateTimeZone.UTC);
    }

    private static String getBucketKeyAsString(DateTime key, DateTimeZone tz) {
        return Joda.forPattern(DateFieldMapper.Defaults.DATE_TIME_FORMATTER.format()).printer().withZone(tz).print(key);
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValuedField_WithTimeZone() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.DAY).minDocCount(1).timeZone("+01:00")).execute()
                .actionGet();
        DateTimeZone tz = DateTimeZone.forID("+01:00");
        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(6));

        DateTime key = new DateTime(2012, 1, 1, 23, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 14, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 14, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 22, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(5);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
    }

    @Test
    public void singleValuedField_OrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.KEY_ASC))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        int i = 0;
        for (Histogram.Bucket bucket : buckets) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.KEY_DESC))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.COUNT_ASC))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.MONTH)
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Object[] propertiesKeys = (Object[]) histo.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) histo.getProperty("_count");
        Object[] propertiesCounts = (Object[]) histo.getProperty("sum.value");

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(1.0));
        assertThat((DateTime) propertiesKeys[0], equalTo(key));
        assertThat((long) propertiesDocCounts[0], equalTo(1l));
        assertThat((double) propertiesCounts[0], equalTo(1.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(5.0));
        assertThat((DateTime) propertiesKeys[1], equalTo(key));
        assertThat((long) propertiesDocCounts[1], equalTo(2l));
        assertThat((double) propertiesCounts[1], equalTo(5.0));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(15.0));
        assertThat((DateTime) propertiesKeys[2], equalTo(key));
        assertThat((long) propertiesDocCounts[2], equalTo(3l));
        assertThat((double) propertiesCounts[2], equalTo(15.0));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.aggregation("sum", true))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.aggregation("sum", false))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationAsc_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.aggregation("stats", "sum", true))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.aggregation("stats", "sum", false))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            assertThat(((DateTime) bucket.getKey()), equalTo(new DateTime(2012, i + 1, 1, 0, 0, DateTimeZone.UTC)));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .script(new Script("new DateTime(_value).plusMonths(1).getMillis()"))
                        .interval(DateHistogramInterval.MONTH)).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }



    /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("dates").interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void multiValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .interval(DateHistogramInterval.MONTH)
                        .order(Histogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        // TODO: use diamond once JI-9019884 is fixed
        List<Histogram.Bucket> buckets = new ArrayList<Histogram.Bucket>(histo.getBuckets());

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(1l));
    }

    /**
     * The script will change to document date values to the following:
     * <p/>
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     */
    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script(new Script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()"))
                        .interval(DateHistogramInterval.MONTH)).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateTime key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    /**
     * The script will change to document date values to the following:
     * <p/>
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     */
    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script(new Script("new DateTime((long)_value, DateTimeZone.UTC).plusMonths(1).getMillis()"))
                        .interval(DateHistogramInterval.MONTH).subAggregation(max("max"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateTime key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    /**
     * Jan 2
     * Feb 2
     * Feb 15
     * Mar 2
     * Mar 15
     * Mar 23
     */
    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script(new Script("doc['date'].value")).interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script(new Script("doc['date'].value")).interval(DateHistogramInterval.MONTH)
                        .subAggregation(max("max"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script(new Script("doc['dates'].values")).interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }



      /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script(new Script("doc['dates'].values")).interval(DateHistogramInterval.MONTH)
                        .subAggregation(max("max"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 2, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(dateHistogram("date_histo").interval(1)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Histogram.Bucket bucket = buckets.get(1);
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("1"));

        Histogram dateHisto = bucket.getAggregations().get("date_histo");
        assertThat(dateHisto, Matchers.notNullValue());
        assertThat(dateHisto.getName(), equalTo("date_histo"));
        assertThat(dateHisto.getBuckets().isEmpty(), is(true));

    }

    @Test
    public void singleValue_WithTimeZone() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(1);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .timeZone("-02:00")
                        .interval(DateHistogramInterval.DAY)
                        .format("yyyy-MM-dd:HH-mm-ssZZ"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("2014-03-10:00-00-00-02:00"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("2014-03-11:00-00-00-02:00"));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValueField_WithExtendedBounds() throws Exception {

        String pattern = "yyyy-MM-dd";
        // we're testing on days, so the base must be rounded to a day
        int interval = randomIntBetween(1, 2); // in days
        long intervalMillis = interval * 24 * 60 * 60 * 1000;
        DateTime base = new DateTime(DateTimeZone.UTC).dayOfMonth().roundFloorCopy();
        DateTime baseKey = new DateTime(intervalMillis * (base.getMillis() / intervalMillis), DateTimeZone.UTC);

        prepareCreate("idx2")
                .setSettings(
                        Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)).execute().actionGet();
        int numOfBuckets = randomIntBetween(3, 6);
        int emptyBucketIndex = randomIntBetween(1, numOfBuckets - 2); // should be in the middle

        long[] docCounts = new long[numOfBuckets];
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numOfBuckets; i++) {
            if (i == emptyBucketIndex) {
                docCounts[i] = 0;
            } else {
                int docCount = randomIntBetween(1, 3);
                for (int j = 0; j < docCount; j++) {
                    DateTime date = baseKey.plusDays(i * interval + randomIntBetween(0, interval - 1));
                    builders.add(indexDoc("idx2", date, j));
                }
                docCounts[i] = docCount;
            }
        }
        indexRandom(true, builders);
        ensureSearchable("idx2");

        DateTime lastDataBucketKey = baseKey.plusDays((numOfBuckets - 1) * interval);

        // randomizing the number of buckets on the min bound
        // (can sometimes fall within the data range, but more frequently will fall before the data range)
        int addedBucketsLeft = randomIntBetween(0, numOfBuckets);
        DateTime boundsMinKey;
        if (frequently()) {
            boundsMinKey = baseKey.minusDays(addedBucketsLeft * interval);
        } else {
            boundsMinKey = baseKey.plusDays(addedBucketsLeft * interval);
            addedBucketsLeft = 0;
        }
        DateTime boundsMin = boundsMinKey.plusDays(randomIntBetween(0, interval - 1));

        // randomizing the number of buckets on the max bound
        // (can sometimes fall within the data range, but more frequently will fall after the data range)
        int addedBucketsRight = randomIntBetween(0, numOfBuckets);
        int boundsMaxKeyDelta = addedBucketsRight * interval;
        if (rarely()) {
            addedBucketsRight = 0;
            boundsMaxKeyDelta = -boundsMaxKeyDelta;
        }
        DateTime boundsMaxKey = lastDataBucketKey.plusDays(boundsMaxKeyDelta);
        DateTime boundsMax = boundsMaxKey.plusDays(randomIntBetween(0, interval - 1));

        // it could be that the random bounds.min we chose ended up greater than
        // bounds.max - this should
        // trigger an error
        boolean invalidBoundsError = boundsMin.isAfter(boundsMax);

        // constructing the newly expected bucket list
        int bucketsCount = numOfBuckets + addedBucketsLeft + addedBucketsRight;
        long[] extendedValueCounts = new long[bucketsCount];
        System.arraycopy(docCounts, 0, extendedValueCounts, addedBucketsLeft, docCounts.length);

        SearchResponse response = null;
        try {
            response = client().prepareSearch("idx2")
                    .addAggregation(dateHistogram("histo")
                            .field("date")
                            .interval(DateHistogramInterval.days(interval))
                            .minDocCount(0)
                                    // when explicitly specifying a format, the extended bounds should be defined by the same format
                            .extendedBounds(format(boundsMin, pattern), format(boundsMax, pattern))
                            .format(pattern))
                    .execute().actionGet();

            if (invalidBoundsError) {
                fail("Expected an exception to be thrown when bounds.min is greater than bounds.max");
                return;
            }

        } catch (Exception e) {
            if (invalidBoundsError) {
                // expected
                return;
            } else {
                throw e;
            }
        }
        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(bucketsCount));

        DateTime key = baseKey.isBefore(boundsMinKey) ? baseKey : boundsMinKey;
        for (int i = 0; i < bucketsCount; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((DateTime) bucket.getKey()), equalTo(key));
            assertThat(bucket.getKeyAsString(), equalTo(format(key, pattern)));
            assertThat(bucket.getDocCount(), equalTo(extendedValueCounts[i]));
            key = key.plusDays(interval);
        }
    }

    /**
     * Test date histogram aggregation with hour interval, timezone shift and
     * extended bounds (see https://github.com/elastic/elasticsearch/issues/12278)
     */
    @Test
    public void singleValueField_WithExtendedBoundsTimezone() throws Exception {

        String index = "test12278";
        prepareCreate(index)
                .setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();

        DateMathParser parser = new DateMathParser(Joda.getStrictStandardDateFormatter());

        final Callable<Long> callable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return System.currentTimeMillis();
            }
        };

        // we pick a random timezone offset of +12/-12 hours and insert two documents
        // one at 00:00 in that time zone and one at 12:00
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int timeZoneHourOffset = randomIntBetween(-12, 12);
        DateTimeZone timezone = DateTimeZone.forOffsetHours(timeZoneHourOffset);
        DateTime timeZoneStartToday = new DateTime(parser.parse("now/d", callable, false, timezone), DateTimeZone.UTC);
        DateTime timeZoneNoonToday = new DateTime(parser.parse("now/d+12h", callable, false, timezone), DateTimeZone.UTC);
        builders.add(indexDoc(index, timeZoneStartToday, 1));
        builders.add(indexDoc(index, timeZoneNoonToday, 2));
        indexRandom(true, builders);
        ensureSearchable(index);

        SearchResponse response = null;
        // retrieve those docs with the same time zone and extended bounds
        response = client()
                .prepareSearch(index)
                .setQuery(QueryBuilders.rangeQuery("date").from("now/d").to("now/d").includeLower(true).includeUpper(true).timeZone(timezone.getID()))
                .addAggregation(
                        dateHistogram("histo").field("date").interval(DateHistogramInterval.hours(1)).timeZone(timezone.getID()).minDocCount(0)
                                .extendedBounds("now/d", "now/d+23h")
                ).execute().actionGet();
        assertSearchResponse(response);

        assertThat("Expected 24 buckets for one day aggregation with hourly interval", response.getHits().totalHits(), equalTo(2l));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(24));

        for (int i = 0; i < buckets.size(); i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat("Bucket " + i + " had wrong key", (DateTime) bucket.getKey(), equalTo(new DateTime(timeZoneStartToday.getMillis() + (i * 60 * 60 * 1000), DateTimeZone.UTC)));
            if (i == 0 || i == 12) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(0l));
            }
        }
        internalCluster().wipeIndices("test12278");
    }

    @Test
    public void singleValue_WithMultipleDateFormatsFromMapping() throws Exception {

        String mappingJson = jsonBuilder().startObject().startObject("type").startObject("properties").startObject("date").field("type", "date").field("format", "dateOptionalTime||dd-MM-yyyy").endObject().endObject().endObject().endObject().string();
        prepareCreate("idx2").addMapping("type", mappingJson).execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", "10-03-2014").endObject());
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .interval(DateHistogramInterval.DAY))
                .execute().actionGet();

        assertSearchHits(response, "0", "1", "2", "3", "4");

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(1));

        DateTime key = new DateTime(2014, 3, 10, 0, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    public void testIssue6965() {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").timeZone("+01:00").interval(DateHistogramInterval.MONTH).minDocCount(0))
                .execute().actionGet();

        assertSearchResponse(response);

        DateTimeZone tz = DateTimeZone.forID("+01:00");

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        DateTime key = new DateTime(2011, 12, 31, 23, 0, DateTimeZone.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 1, 31, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 2, 29, 23, 0, DateTimeZone.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(getBucketKeyAsString(key, tz)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    public void testDSTBoundaryIssue9491() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test9491").addMapping("type", "d", "type=date").get());
        indexRandom(true, client().prepareIndex("test9491", "type").setSource("d", "2014-10-08T13:00:00Z"),
                client().prepareIndex("test9491", "type").setSource("d", "2014-11-08T13:00:00Z"));
        ensureSearchable("test9491");
        SearchResponse response = client().prepareSearch("test9491")
                .addAggregation(dateHistogram("histo").field("d").interval(DateHistogramInterval.YEAR).timeZone("Asia/Jerusalem"))
                .execute().actionGet();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(1));
        assertThat(histo.getBuckets().get(0).getKeyAsString(), equalTo("2014-01-01T00:00:00.000+02:00"));
        internalCluster().wipeIndices("test9491");
    }

    public void testIssue8209() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test8209").addMapping("type", "d", "type=date").get());
        indexRandom(true,
                client().prepareIndex("test8209", "type").setSource("d", "2014-01-01T00:00:00Z"),
                client().prepareIndex("test8209", "type").setSource("d", "2014-04-01T00:00:00Z"),
                client().prepareIndex("test8209", "type").setSource("d", "2014-04-30T00:00:00Z"));
        ensureSearchable("test8209");
        SearchResponse response = client().prepareSearch("test8209")
                .addAggregation(dateHistogram("histo").field("d").interval(DateHistogramInterval.MONTH).timeZone("CET")
                        .minDocCount(0))
                .execute().actionGet();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
        assertThat(histo.getBuckets().get(0).getKeyAsString(), equalTo("2014-01-01T00:00:00.000+01:00"));
        assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(histo.getBuckets().get(1).getKeyAsString(), equalTo("2014-02-01T00:00:00.000+01:00"));
        assertThat(histo.getBuckets().get(1).getDocCount(), equalTo(0L));
        assertThat(histo.getBuckets().get(2).getKeyAsString(), equalTo("2014-03-01T00:00:00.000+01:00"));
        assertThat(histo.getBuckets().get(2).getDocCount(), equalTo(0L));
        assertThat(histo.getBuckets().get(3).getKeyAsString(), equalTo("2014-04-01T00:00:00.000+02:00"));
        assertThat(histo.getBuckets().get(3).getDocCount(), equalTo(2L));
        internalCluster().wipeIndices("test8209");
    }

    /**
     * see issue #9634, negative interval in date_histogram should raise exception
     */
    public void testExceptionOnNegativeInterval() {
        try {
            client().prepareSearch("idx")
                    .addAggregation(dateHistogram("histo").field("date").interval(-TimeUnit.DAYS.toMillis(1)).minDocCount(0)).execute()
                    .actionGet();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("ElasticsearchParseException"));
        }
    }

    public void testTimestampField() { // see #11692
        SearchResponse response = client().prepareSearch("idx").addAggregation(dateHistogram("histo").field("_timestamp").interval(randomFrom(DateHistogramInterval.DAY, DateHistogramInterval.MONTH))).get();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), greaterThan(0));
    }
}
