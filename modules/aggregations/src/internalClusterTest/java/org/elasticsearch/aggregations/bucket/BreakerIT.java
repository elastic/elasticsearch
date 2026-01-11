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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.LongBounds;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BreakerIT extends AggregationIntegTestCase {
    @Before
    public void initIndex() {
        prepareCreate("test").setMapping("@timestamp", "type=date", "n", "type=long", "kwd", "type=keyword").get(TEST_REQUEST_TIMEOUT);
        List<IndexRequestBuilder> index = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            index.add(client().prepareIndex("test").setSource("@timestamp", i, "n", i, "kwd", "k" + i));
        }
        indexRandom(true, index);
    }

    public void testSmallHisto() {
        SearchResponse response = prepareSearch().addAggregation(
            histogram(10).subAggregation(
                histogram(10).subAggregation(histogram(10).subAggregation(new MaxAggregationBuilder("m").field("n")))
                    .subAggregation(new MaxBucketPipelineAggregationBuilder("max", "n > m"))
            )
        ).get(TEST_REQUEST_TIMEOUT);
        try {
            Histogram histo = response.getAggregations().get("n");
            assertThat(
                histo.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).toList(),
                equalTo(List.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"))
            );
        } finally {
            response.decRef();
        }
    }

    public void testSmallDateHisto() {
        SearchResponse response = prepareSearch().addAggregation(
            dateHistogram("1970-01-01T00:10:00Z").subAggregation(
                dateHistogram("1970-01-01T00:10:00Z").subAggregation(
                    dateHistogram("1970-01-01T00:10:00Z").subAggregation(new MaxAggregationBuilder("m").field("n"))
                ).subAggregation(new MaxBucketPipelineAggregationBuilder("max", "d > m"))
            )
        ).get(TEST_REQUEST_TIMEOUT);
        try {
            InternalDateHistogram histo = response.getAggregations().get("d");
            assertThat(
                histo.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).toList(),
                equalTo(
                    List.of(
                        "1970-01-01T00:00:00.000Z",
                        "1970-01-01T00:01:00.000Z",
                        "1970-01-01T00:02:00.000Z",
                        "1970-01-01T00:03:00.000Z",
                        "1970-01-01T00:04:00.000Z",
                        "1970-01-01T00:05:00.000Z",
                        "1970-01-01T00:06:00.000Z",
                        "1970-01-01T00:07:00.000Z",
                        "1970-01-01T00:08:00.000Z",
                        "1970-01-01T00:09:00.000Z",
                        "1970-01-01T00:10:00.000Z"
                    )
                )
            );
        } finally {
            response.decRef();
        }
    }

    public void testBigHisto() {
        Exception e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> prepareSearch().addAggregation(
                terms().subAggregation(
                    histogram(1000).subAggregation(
                        histogram(1000).subAggregation(histogram(1000).subAggregation(new MaxAggregationBuilder("m").field("n")))
                    )
                )
            ).get(TEST_REQUEST_TIMEOUT)
        );
        assertThat(e.getCause().getMessage(), containsString("Trying to create too many buckets. Must be less than or equal to: [65536]"));
    }

    public void testBigDateHisto() {
        Exception e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> prepareSearch().addAggregation(
                terms().subAggregation(
                    dateHistogram("1970-01-01T20:00:00Z").subAggregation(
                        dateHistogram("1970-01-01T20:00:00Z").subAggregation(
                            dateHistogram("1970-01-01T20:00:00Z").subAggregation(new MaxAggregationBuilder("m").field("n"))
                        )
                    )
                )
            ).get(TEST_REQUEST_TIMEOUT)
        );
        assertThat(e.getCause().getMessage(), containsString("Trying to create too many buckets. Must be less than or equal to: [65536]"));
    }

    private TermsAggregationBuilder terms() {
        return new TermsAggregationBuilder("kwd").field("kwd");
    }

    private HistogramAggregationBuilder histogram(int max) {
        return new HistogramAggregationBuilder("n").field("n").interval(1).extendedBounds(0, max).format("");
    }

    private DateHistogramAggregationBuilder dateHistogram(String max) {
        return new DateHistogramAggregationBuilder("d").field("@timestamp")
            .fixedInterval(DateHistogramInterval.MINUTE)
            .extendedBounds(new LongBounds("1970-01-01T00:00:00Z", max));
    }
}
