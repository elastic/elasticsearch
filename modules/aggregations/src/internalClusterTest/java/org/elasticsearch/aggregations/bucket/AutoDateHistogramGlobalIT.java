/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests auto date histograms inside global aggs
 */
public class AutoDateHistogramGlobalIT extends AggregationIntegTestCase {

    private static final String INDEX = "auto_date_histogram_global_idx";
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

    public void testAutoDateHistogramInsideGlobalWithRangeFilter() {
        runGlobalAutoDateHistogramTest(rangeQuery(DATE_FIELD).gte("2018-01-01T00:00:00Z"));
    }

    public void testAutoDateHistogramInsideGlobalWithBoolMustQuery() {
        runGlobalAutoDateHistogramTest(boolQuery().must(rangeQuery(DATE_FIELD).gte("2018-01-01T00:00:00Z")));
    }

    private void runGlobalAutoDateHistogramTest(QueryBuilder query) {
        final long expectedTotalDocs = totalDocs;
        final long earliestMillis = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        final long latestMillis = ZonedDateTime.of(2020, 12, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertNoFailuresAndResponse(
            prepareSearch(INDEX).setQuery(query)
                .setSize(0)
                .addAggregation(
                    global("everything").subAggregation(
                        new AutoDateHistogramAggregationBuilder("by_month").field(DATE_FIELD).setNumBuckets(10)
                    )
                ),
            response -> {
                SingleBucketAggregation everything = response.getAggregations().get("everything");
                assertThat(everything, notNullValue());
                assertThat(everything.getDocCount(), equalTo(expectedTotalDocs));

                Histogram histogram = everything.getAggregations().get("by_month");
                assertThat(histogram, notNullValue());
                assertThat(histogram.getBuckets().size(), greaterThan(0));

                long sum = histogram.getBuckets().stream().mapToLong(Histogram.Bucket::getDocCount).sum();
                assertThat(sum, equalTo(expectedTotalDocs));

                long firstKey = ((ZonedDateTime) histogram.getBuckets().get(0).getKey()).toInstant().toEpochMilli();
                long lastKey = ((ZonedDateTime) histogram.getBuckets().get(histogram.getBuckets().size() - 1).getKey()).toInstant()
                    .toEpochMilli();
                assertThat("first bucket key must be at or before the earliest doc", firstKey, lessThanOrEqualTo(earliestMillis));
                assertThat("last bucket key must be at or before the latest doc", lastKey, lessThanOrEqualTo(latestMillis));
            }
        );
    }
}
