/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed.extractor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ExtractorUtilsTests extends ESTestCase {

    public void testGetHistogramAggregation_DateHistogramHasSibling() {
        AvgAggregationBuilder avg = AggregationBuilders.avg("avg");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time");

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> ExtractorUtils.getHistogramAggregation(
                new AggregatorFactories.Builder().addAggregator(avg).addAggregator(dateHistogram).getAggregatorFactories()
            )
        );
        assertEquals("The date_histogram (or histogram) aggregation cannot have sibling aggregations", e.getMessage());

        TermsAggregationBuilder terms = AggregationBuilders.terms("terms");
        terms.subAggregation(dateHistogram);
        terms.subAggregation(avg);
        e = expectThrows(
            ElasticsearchException.class,
            () -> ExtractorUtils.getHistogramAggregation(new AggregatorFactories.Builder().addAggregator(terms).getAggregatorFactories())
        );
        assertEquals("The date_histogram (or histogram) aggregation cannot have sibling aggregations", e.getMessage());
    }

    public void testGetHistogramAggregation() {
        AvgAggregationBuilder avg = AggregationBuilders.avg("avg");
        TermsAggregationBuilder nestedTerms = AggregationBuilders.terms("nested_terms");

        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time");
        AggregationBuilder histogramAggregationBuilder = ExtractorUtils.getHistogramAggregation(
            new AggregatorFactories.Builder().addAggregator(dateHistogram).getAggregatorFactories()
        );
        assertEquals(dateHistogram, histogramAggregationBuilder);

        dateHistogram.subAggregation(avg).subAggregation(nestedTerms);
        histogramAggregationBuilder = ExtractorUtils.getHistogramAggregation(
            new AggregatorFactories.Builder().addAggregator(dateHistogram).getAggregatorFactories()
        );
        assertEquals(dateHistogram, histogramAggregationBuilder);

        TermsAggregationBuilder toplevelTerms = AggregationBuilders.terms("top_level");
        toplevelTerms.subAggregation(dateHistogram);
        histogramAggregationBuilder = ExtractorUtils.getHistogramAggregation(
            new AggregatorFactories.Builder().addAggregator(toplevelTerms).getAggregatorFactories()
        );

        assertEquals(dateHistogram, histogramAggregationBuilder);
    }

    public void testGetHistogramAggregation_MissingHistogramAgg() {
        TermsAggregationBuilder terms = AggregationBuilders.terms("top_level");
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> ExtractorUtils.getHistogramAggregation(new AggregatorFactories.Builder().addAggregator(terms).getAggregatorFactories())
        );
        assertEquals("A date_histogram (or histogram) aggregation is required", e.getMessage());
    }

    public void testGetHistogramIntervalMillis_GivenDateHistogramWithInvalidTimeZone() {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("bucket")
            .field("time")
            .fixedInterval(new DateHistogramInterval(300000 + "ms"))
            .timeZone(ZoneId.of("CET"))
            .subAggregation(maxTime);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> ExtractorUtils.getHistogramIntervalMillis(dateHistogram)
        );

        assertThat(e.getMessage(), equalTo("ML requires date_histogram.time_zone to be UTC"));
    }

    public void testGetHistogramIntervalMillis_GivenUtcTimeZonesDeprecated() {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        ZoneId zone = randomFrom(ZoneOffset.UTC, ZoneId.of("UTC"));
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("bucket")
            .field("time")
            .fixedInterval(new DateHistogramInterval(300000L + "ms"))
            .timeZone(zone)
            .subAggregation(maxTime);
        assertThat(ExtractorUtils.getHistogramIntervalMillis(dateHistogram), is(300_000L));
    }

    public void testGetHistogramIntervalMillis_GivenUtcTimeZones() {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        ZoneId zone = randomFrom(ZoneOffset.UTC, ZoneId.of("UTC"));
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("bucket")
            .field("time")
            .fixedInterval(new DateHistogramInterval("300000ms"))
            .timeZone(zone)
            .subAggregation(maxTime);
        assertThat(ExtractorUtils.getHistogramIntervalMillis(dateHistogram), is(300_000L));
    }

    public void testIsHistogram() {
        assertTrue(ExtractorUtils.isHistogram(AggregationBuilders.dateHistogram("time")));
        assertTrue(ExtractorUtils.isHistogram(AggregationBuilders.histogram("time")));
        assertFalse(ExtractorUtils.isHistogram(AggregationBuilders.max("time")));
    }

    public void testValidateAndGetCalendarInterval() {
        assertEquals(300 * 1000L, ExtractorUtils.validateAndGetCalendarInterval("5m"));
        assertEquals(7200 * 1000L, ExtractorUtils.validateAndGetCalendarInterval("2h"));
        assertEquals(86400L * 1000L, ExtractorUtils.validateAndGetCalendarInterval("1d"));
    }

    public void testValidateAndGetCalendarInterval_intervalIsLongerThanAWeek() {
        expectThrows(ElasticsearchException.class, () -> ExtractorUtils.validateAndGetCalendarInterval("8d"));
    }
}
