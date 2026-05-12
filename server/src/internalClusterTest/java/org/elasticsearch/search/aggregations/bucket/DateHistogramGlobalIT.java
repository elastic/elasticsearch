/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests date histograms inside global aggs
 */
public class DateHistogramGlobalIT extends ESIntegTestCase {

    private static final String INDEX = "date_histogram_global_idx";
    private static final String DATE_FIELD = "@timestamp";

    private int totalDocs;

    @Before
    public void setUpIndex() {
        assertAcked(prepareCreate(INDEX).setMapping(DATE_FIELD, "type=date"));

        // Index one doc per month from 2010 through 2020
        List<IndexRequestBuilder> docs = new ArrayList<>();
        totalDocs = 0;
        for (int year = 2010; year <= 2020; year++) {
            for (int month = 1; month <= 12; month++) {
                docs.add(prepareIndex(INDEX).setSource(DATE_FIELD, String.format(Locale.ROOT, "%04d-%02d-01T00:00:00Z", year, month)));
                totalDocs++;
            }
        }
        indexRandom(true, docs);
        ensureSearchable(INDEX);
    }

    public void testDateHistogramInsideGlobalWithRangeFilter() {
        runGlobalDateHistogramTest(rangeQuery(DATE_FIELD).gte("2018-01-01T00:00:00Z"));
    }

    public void testDateHistogramInsideGlobalWithRangeFilterBothBounds() {
        runGlobalDateHistogramTest(rangeQuery(DATE_FIELD).gte("2018-01-01T00:00:00Z").lt("2019-01-01T00:00:00Z"));
    }

    public void testDateHistogramInsideGlobalWithBoolMustQuery() {
        runGlobalDateHistogramTest(boolQuery().must(rangeQuery(DATE_FIELD).gte("2018-01-01T00:00:00Z")));
    }

    private void runGlobalDateHistogramTest(QueryBuilder query) {
        final long expectedTotalDocs = totalDocs;
        final List<Long> expectedKeys = new ArrayList<>();
        for (int year = 2010; year <= 2020; year++) {
            for (int month = 1; month <= 12; month++) {
                expectedKeys.add(ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli());
            }
        }
        assertNoFailuresAndResponse(
            prepareSearch(INDEX).setQuery(query)
                .setSize(0)
                .addAggregation(
                    global("everything").subAggregation(
                        dateHistogram("by_month").field(DATE_FIELD).calendarInterval(DateHistogramInterval.MONTH)
                    )
                ),
            response -> {
                SingleBucketAggregation everything = response.getAggregations().get("everything");
                assertThat(everything, notNullValue());
                assertThat(everything.getDocCount(), equalTo(expectedTotalDocs));

                Histogram histogram = everything.getAggregations().get("by_month");
                assertThat(histogram, notNullValue());
                assertThat("histogram has wrong number of buckets", (long) histogram.getBuckets().size(), equalTo(expectedTotalDocs));
                for (int i = 0; i < expectedKeys.size(); i++) {
                    Histogram.Bucket bucket = histogram.getBuckets().get(i);
                    long actualKey = ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli();
                    assertThat("bucket at index " + i + " has wrong key", actualKey, equalTo(expectedKeys.get(i)));
                    assertThat("bucket " + bucket.getKeyAsString() + " has wrong doc count", bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }
}
