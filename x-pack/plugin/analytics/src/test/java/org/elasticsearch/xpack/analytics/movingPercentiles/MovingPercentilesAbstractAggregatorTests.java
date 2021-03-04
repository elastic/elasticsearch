/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.movingPercentiles;


import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public abstract class MovingPercentilesAbstractAggregatorTests extends AggregatorTestCase {

    protected static final String DATE_FIELD = "date";
    protected static final String INSTANT_FIELD = "instant";
    protected static final String VALUE_FIELD = "value_field";

    protected static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45",
        "2017-01-02T03:43:34",
        "2017-01-03T04:11:00",
        "2017-01-04T05:11:31",
        "2017-01-05T08:24:05",
        "2017-01-06T13:09:32",
        "2017-01-07T13:47:43",
        "2017-01-08T16:14:34",
        "2017-01-09T17:09:50",
        "2017-01-10T22:55:46",
        "2017-01-11T22:55:46",
        "2017-01-12T22:55:46",
        "2017-01-13T22:55:46",
        "2017-01-14T22:55:46",
        "2017-01-15T22:55:46",
        "2017-01-16T22:55:46",
        "2017-01-17T22:55:46",
        "2017-01-18T22:55:46",
        "2017-01-19T22:55:46",
        "2017-01-20T22:55:46");


    public void testMatchAllDocs() throws IOException {
        check(randomIntBetween(0, 10), randomIntBetween(1, 25));
    }

    private void check(int shift, int window) throws IOException {
        MovingPercentilesPipelineAggregationBuilder builder =
            new MovingPercentilesPipelineAggregationBuilder("MovingPercentiles", "percentiles", window);
        builder.setShift(shift);

        Query query = new MatchAllDocsQuery();
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(DATE_FIELD);

        aggBuilder.subAggregation(new PercentilesAggregationBuilder("percentiles").field(VALUE_FIELD)
            .percentilesConfig(getPercentileConfig()));
        aggBuilder.subAggregation(builder);

        executeTestCase(window, shift, query, aggBuilder);
    }

    protected abstract PercentilesConfig getPercentileConfig();

    protected abstract void executeTestCase(int window, int shift, Query query,
                                 DateHistogramAggregationBuilder aggBuilder) throws IOException;


    protected int clamp(int index, int length) {
        if (index < 0) {
            return 0;
        }
        if (index > length) {
            return length;
        }
        return index;
    }

    protected static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}
