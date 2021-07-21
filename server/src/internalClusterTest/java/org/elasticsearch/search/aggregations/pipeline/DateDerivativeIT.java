/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.derivative;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class DateDerivativeIT extends ESIntegTestCase {

    // some index names used during these tests
    private static final String IDX_DST_START = "idx_dst_start";
    private static final String IDX_DST_END = "idx_dst_end";
    private static final String IDX_DST_KATHMANDU = "idx_dst_kathmandu";

    private ZonedDateTime date(int month, int day) {
        return ZonedDateTime.of(2012, month, day, 0, 0, 0, 0, ZoneOffset.UTC);
    }

    private static IndexRequestBuilder indexDoc(String idx, ZonedDateTime date, int value) throws Exception {
        return client().prepareIndex(idx).setSource(
                jsonBuilder().startObject().timeField("date", date).field("value", value).endObject());
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx").setSource(
                jsonBuilder().startObject().field("value", value).timeField("date", date(month, day)).startArray("dates")
                        .timeValue(date(month, day)).timeValue(date(month + 1, day + 1)).endArray().endObject());
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        // TODO: would be nice to have more random data here
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx").setId("" + i).setSource(
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
        internalCluster().wipeIndices(IDX_DST_START, IDX_DST_END, IDX_DST_KATHMANDU);
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count"))).get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        ZonedDateTime key = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1L));
        SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, nullValue());

        key = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2L));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));

        key = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));
    }

    public void testSingleValuedFieldNormalised() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count").unit(DateHistogramInterval.DAY))).get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        ZonedDateTime key = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1L));
        Derivative docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, nullValue());

        key = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2L));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), closeTo(1d, 0.00001));
        assertThat(docCountDeriv.normalizedValue(), closeTo(1d / 31d, 0.00001));

        key = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), closeTo(1d, 0.00001));
        assertThat(docCountDeriv.normalizedValue(), closeTo(1d / 29d, 0.00001));
    }

    /**
     * Do a derivative on a date histogram with time zone CET at DST start
     */
    public void testSingleValuedFieldNormalised_timeZone_CET_DstStart() throws Exception {
        createIndex(IDX_DST_START);
        List<IndexRequestBuilder> builders = new ArrayList<>();

        ZoneId timezone = ZoneId.of("CET");
        DateFormatter formatter = DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss").withZone(timezone);
        // epoch millis: 1332547200000
        addNTimes(1, IDX_DST_START, DateFormatters.from(formatter.parse("2012-03-24T01:00:00")), builders);
        // day with dst shift, only 23h long
        addNTimes(2, IDX_DST_START, DateFormatters.from(formatter.parse("2012-03-25T01:00:00")), builders);
        addNTimes(3, IDX_DST_START, DateFormatters.from(formatter.parse("2012-03-26T01:00:00")), builders);
        addNTimes(4, IDX_DST_START, DateFormatters.from(formatter.parse("2012-03-27T01:00:00")), builders);
        indexRandom(true, builders);
        ensureSearchable();

        SearchResponse response = client()
                .prepareSearch(IDX_DST_START)
                .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.DAY)
                        .timeZone(timezone).minDocCount(0)
                        .subAggregation(derivative("deriv", "_count").unit(DateHistogramInterval.HOUR)))
                .get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu-MM-dd");
        ZonedDateTime expectedKeyFirstBucket =
            LocalDate.from(dateFormatter.parse("2012-03-24")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(0), expectedKeyFirstBucket, 1L, nullValue(), null, null);

        ZonedDateTime expectedKeySecondBucket =
            LocalDate.from(dateFormatter.parse("2012-03-25")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(1), expectedKeySecondBucket,2L, notNullValue(), 1d, 1d / 24d);

        // the following is normalized using a 23h bucket width
        ZonedDateTime expectedKeyThirdBucket =
            LocalDate.from(dateFormatter.parse("2012-03-26")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(2), expectedKeyThirdBucket, 3L, notNullValue(), 1d, 1d / 23d);

        ZonedDateTime expectedKeyFourthBucket =
            LocalDate.from(dateFormatter.parse("2012-03-27")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(3), expectedKeyFourthBucket, 4L, notNullValue(), 1d, 1d / 24d);
    }

    /**
     * Do a derivative on a date histogram with time zone CET at DST end
     */
    public void testSingleValuedFieldNormalised_timeZone_CET_DstEnd() throws Exception {
        createIndex(IDX_DST_END);
        ZoneId timezone = ZoneId.of("CET");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        DateFormatter formatter = DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss").withZone(timezone);
        addNTimes(1, IDX_DST_END, DateFormatters.from(formatter.parse("2012-10-27T01:00:00")), builders);
        // day with dst shift -1h, 25h long
        addNTimes(2, IDX_DST_END, DateFormatters.from(formatter.parse("2012-10-28T01:00:00")), builders);
        addNTimes(3, IDX_DST_END, DateFormatters.from(formatter.parse("2012-10-29T01:00:00")), builders);
        addNTimes(4, IDX_DST_END, DateFormatters.from(formatter.parse("2012-10-30T01:00:00")), builders);
        indexRandom(true, builders);
        ensureSearchable();

        SearchResponse response = client()
                .prepareSearch(IDX_DST_END)
                .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.DAY)
                        .timeZone(timezone).minDocCount(0)
                        .subAggregation(derivative("deriv", "_count").unit(DateHistogramInterval.HOUR)))
                .get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu-MM-dd").withZone(ZoneOffset.UTC);

        ZonedDateTime expectedKeyFirstBucket =
            LocalDate.from(dateFormatter.parse("2012-10-27")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(0), expectedKeyFirstBucket, 1L, nullValue(), null, null);

        ZonedDateTime expectedKeySecondBucket =
            LocalDate.from(dateFormatter.parse("2012-10-28")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(1), expectedKeySecondBucket, 2L, notNullValue(), 1d, 1d / 24d);

        // the following is normalized using a 25h bucket width
        ZonedDateTime expectedKeyThirdBucket =
            LocalDate.from(dateFormatter.parse("2012-10-29")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(2), expectedKeyThirdBucket, 3L, notNullValue(), 1d, 1d / 25d);

        ZonedDateTime expectedKeyFourthBucket =
            LocalDate.from(dateFormatter.parse("2012-10-30")).atStartOfDay(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(3), expectedKeyFourthBucket, 4L, notNullValue(), 1d, 1d / 24d);
    }

    /**
     * also check for time zone shifts that are not one hour, e.g.
     * "Asia/Kathmandu, 1 Jan 1986 - Time Zone Change (IST → NPT), at 00:00:00 clocks were turned forward 00:15 minutes
     */
    public void testSingleValuedFieldNormalised_timeZone_AsiaKathmandu() throws Exception {
        createIndex(IDX_DST_KATHMANDU);
        ZoneId timezone = ZoneId.of("Asia/Kathmandu");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        DateFormatter formatter = DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss").withZone(timezone);
        addNTimes(1, IDX_DST_KATHMANDU, DateFormatters.from(formatter.parse("1985-12-31T22:30:00")), builders);
        // the shift happens during the next bucket, which includes the 45min that do not start on the full hour
        addNTimes(2, IDX_DST_KATHMANDU, DateFormatters.from(formatter.parse("1985-12-31T23:30:00")), builders);
        addNTimes(3, IDX_DST_KATHMANDU, DateFormatters.from(formatter.parse("1986-01-01T01:30:00")), builders);
        addNTimes(4, IDX_DST_KATHMANDU, DateFormatters.from(formatter.parse("1986-01-01T02:30:00")), builders);
        indexRandom(true, builders);
        ensureSearchable();

        SearchResponse response = client()
                .prepareSearch(IDX_DST_KATHMANDU)
                .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.HOUR)
                        .timeZone(timezone).minDocCount(0)
                        .subAggregation(derivative("deriv", "_count").unit(DateHistogramInterval.MINUTE)))
                .get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

        ZonedDateTime expectedKeyFirstBucket =
            LocalDateTime.from(dateFormatter.parse("1985-12-31T22:00:00")).atZone(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(0), expectedKeyFirstBucket, 1L, nullValue(), null,null);

        ZonedDateTime expectedKeySecondBucket =
            LocalDateTime.from(dateFormatter.parse("1985-12-31T23:00:00")).atZone(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(1), expectedKeySecondBucket, 2L, notNullValue(), 1d,1d / 60d);

        // the following is normalized using a 105min bucket width
        ZonedDateTime expectedKeyThirdBucket =
            LocalDateTime.from(dateFormatter.parse("1986-01-01T01:00:00")).atZone(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(2), expectedKeyThirdBucket, 3L, notNullValue(), 1d,1d / 105d);

        ZonedDateTime expectedKeyFourthBucket =
            LocalDateTime.from(dateFormatter.parse("1986-01-01T02:00:00")).atZone(timezone).withZoneSameInstant(ZoneOffset.UTC);
        assertBucket(buckets.get(3), expectedKeyFourthBucket, 4L, notNullValue(), 1d,1d / 60d);
    }

    private static void addNTimes(int amount, String index, ZonedDateTime dateTime, List<IndexRequestBuilder> builders) throws Exception {
        for (int i = 0; i < amount; i++) {
            builders.add(indexDoc(index, dateTime, 1));
        }
    }

    private static void assertBucket(Histogram.Bucket bucket, ZonedDateTime expectedKey, long expectedDocCount,
            Matcher<Object> derivativeMatcher, Double derivative, Double normalizedDerivative) {
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(expectedKey));
        assertThat(bucket.getDocCount(), equalTo(expectedDocCount));
        Derivative docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, derivativeMatcher);
        if (docCountDeriv != null) {
            assertThat(docCountDeriv.value(), closeTo(derivative, 0.00001));
            assertThat(docCountDeriv.normalizedValue(), closeTo(normalizedDerivative, 0.00001));
        }
    }

    public void testSingleValuedFieldWithSubAggregation() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                            .subAggregation(sum("sum").field("value")).subAggregation(derivative("deriv", "sum")))
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation)histo).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation)histo).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation)histo).getProperty("sum.value");

        ZonedDateTime key = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(1.0));
        SimpleValue deriv = bucket.getAggregations().get("deriv");
        assertThat(deriv, nullValue());
        assertThat((ZonedDateTime) propertiesKeys[0], equalTo(key));
        assertThat((long) propertiesDocCounts[0], equalTo(1L));
        assertThat((double) propertiesCounts[0], equalTo(1.0));

        key = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(5.0));
        deriv = bucket.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.value(), equalTo(4.0));
        assertThat(((InternalMultiBucketAggregation.InternalBucket)bucket).getProperty(
                "histo", AggregationPath.parse("deriv.value").getPathElementsAsStringList()), equalTo(4.0));
        assertThat((ZonedDateTime) propertiesKeys[1], equalTo(key));
        assertThat((long) propertiesDocCounts[1], equalTo(2L));
        assertThat((double) propertiesCounts[1], equalTo(5.0));

        key = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(15.0));
        deriv = bucket.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.value(), equalTo(10.0));
        assertThat(((InternalMultiBucketAggregation.InternalBucket)bucket).getProperty(
                "histo", AggregationPath.parse("deriv.value").getPathElementsAsStringList()), equalTo(10.0));
        assertThat((ZonedDateTime) propertiesKeys[2], equalTo(key));
        assertThat((long) propertiesDocCounts[2], equalTo(3L));
        assertThat((double) propertiesCounts[2], equalTo(15.0));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        dateHistogram("histo").field("dates").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count"))).get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(4));

        ZonedDateTime key = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(true));
        SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, nullValue());

        key = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(2.0));

        key = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0,ZoneOffset.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(2.0));

        key = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-2.0));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count"))).get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.MONTH).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count"))).get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        ZonedDateTime key = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(true));
        SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, nullValue());

        key = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));

        key = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((ZonedDateTime) bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        docCountDeriv = bucket.getAggregations().get("deriv");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1.0));
    }

}
