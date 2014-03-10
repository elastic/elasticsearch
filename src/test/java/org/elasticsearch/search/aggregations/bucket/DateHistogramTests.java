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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class DateHistogramTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return between(0, 1);
    }

    private DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private DateTime date(String date) {
        return DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date);
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        // TODO: would be nice to have more random data here
        indexRandom(true,
                indexDoc(1, 2, 1),  // date: Jan 2, dates: Jan 2, Feb 3
                indexDoc(2, 2, 2),  // date: Feb 2, dates: Feb 2, Mar 3
                indexDoc(2, 15, 3), // date: Feb 15, dates: Feb 15, Mar 16
                indexDoc(3, 2, 4),  // date: Mar 2, dates: Mar 2, Apr 3
                indexDoc(3, 15, 5), // date: Mar 15, dates: Mar 15, Apr 16
                indexDoc(3, 23, 6)); // date: Mar 23, dates: Mar 23, Apr 24
        ensureSearchable();
    }

    private static DateHistogram.Bucket getBucket(DateHistogram histogram, DateTime key) {
        return getBucket(histogram, key, DateFieldMapper.Defaults.DATE_TIME_FORMATTER.format());
    }

    private static DateHistogram.Bucket getBucket(DateHistogram histogram, DateTime key, String format) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return histogram.getBucketByKey(key);
            }
            return histogram.getBucketByKey(key.getMillis());
        }
        if (randomBoolean()) {
            return histogram.getBucketByKey("" + key.getMillis());
        }
        return histogram.getBucketByKey(Joda.forPattern(format).printer().print(key));
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        DateHistogram.Bucket bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValuedField_WithPostTimeZone() throws Exception {
        SearchResponse response;
        if (randomBoolean()) {
            response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.DAY).postZone("-01:00"))
                .execute().actionGet();
        } else {

            // checking post_zone setting as an int

            response = client().prepareSearch("idx")
                .addAggregation(new AbstractAggregationBuilder("histo", "date_histogram") {
                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        return builder.startObject(name)
                                .startObject(type)
                                    .field("field", "date")
                                    .field("interval", "1d")
                                    .field("post_zone", -1)
                                .endObject()
                            .endObject();
                    }
                })
                .execute().actionGet();
        }

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(6));

        long key = new DateTime(2012, 1, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 15, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 15, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 23, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
    }

    @Test
    public void singleValuedField_OrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_ASC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_ASC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                    .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(1.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(5.0));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(15.0));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
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
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", true))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", false))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationAsc_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", true))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", false))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .script("new DateTime(_value).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
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
                .addAggregation(dateHistogram("histo").field("dates").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void multiValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(histo.getBuckets());

        DateHistogram.Bucket bucket = buckets.get(0);
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
     *
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
                        .script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    /**
     * The script will change to document date values to the following:
     *
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     *
     */
    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
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
                .addAggregation(dateHistogram("histo").script("doc['date'].value").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script("doc['date'].value")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script("doc['dates'].values").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
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
                        .script("doc['dates'].values")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 2, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(dateHistogram("date_histo").interval(1)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        DateHistogram dateHisto = bucket.getAggregations().get("date_histo");
        assertThat(dateHisto, Matchers.notNullValue());
        assertThat(dateHisto.getName(), equalTo("date_histo"));
        assertThat(dateHisto.getBuckets().isEmpty(), is(true));

    }

    @Test
    public void singleValue_WithPreZone() throws Exception {
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
                        .preZone("-2:00")
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValue_WithPreZone_WithAadjustLargeInterval() throws Exception {
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
                        .preZone("-2:00")
                        .interval(DateHistogram.Interval.DAY)
                        .preZoneAdjustLargeInterval(true)
                        .format("yyyy-MM-dd'T'HH:mm:ss"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10T02:00:00");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11T02:00:00");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }
}
