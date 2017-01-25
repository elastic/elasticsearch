/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.Term;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createAggs;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createSingleValue;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationToJsonProcessorTests extends ESTestCase {

    public void testProcessGivenHistogramOnly() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3),
                createHistogramBucket(2000L, 5)
        );
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("time");
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        String json = aggToString(histogram);

        assertThat(json, equalTo("{\"time\":1000,\"doc_count\":3} {\"time\":2000,\"doc_count\":5}"));
    }

    public void testProcessGivenSingleMetricPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Arrays.asList(createSingleValue("my_value", 1.0))),
                createHistogramBucket(2000L, 5, Arrays.asList(createSingleValue("my_value", 2.0)))
        );
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("time");
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        String json = aggToString(histogram);

        assertThat(json, equalTo("{\"time\":1000,\"my_value\":1.0,\"doc_count\":3} {\"time\":2000,\"my_value\":2.0,\"doc_count\":5}"));
    }

    public void testProcessGivenTermsPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createTerms("my_field", new Term("a", 1), new Term("b", 2), new Term("c", 1)))),
                createHistogramBucket(2000L, 5, Arrays.asList(createTerms("my_field", new Term("a", 5), new Term("b", 2)))),
                createHistogramBucket(3000L, 0, Arrays.asList()),
                createHistogramBucket(4000L, 7, Arrays.asList(createTerms("my_field", new Term("c", 4), new Term("b", 3))))
        );
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("time");
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        String json = aggToString(histogram);

        assertThat(json, equalTo("{\"time\":1000,\"my_field\":\"a\",\"doc_count\":1} " +
                "{\"time\":1000,\"my_field\":\"b\",\"doc_count\":2} " +
                "{\"time\":1000,\"my_field\":\"c\",\"doc_count\":1} " +
                "{\"time\":2000,\"my_field\":\"a\",\"doc_count\":5} " +
                "{\"time\":2000,\"my_field\":\"b\",\"doc_count\":2} " +
                "{\"time\":4000,\"my_field\":\"c\",\"doc_count\":4} " +
                "{\"time\":4000,\"my_field\":\"b\",\"doc_count\":3}"));
    }

    public void testProcessGivenSingleMetricPerSingleTermsPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(createTerms("my_field",
                        new Term("a", 1, "my_value", 11.0), new Term("b", 2, "my_value", 12.0), new Term("c", 1, "my_value", 13.0)))),
                createHistogramBucket(2000L, 5, Arrays.asList(createTerms("my_field",
                        new Term("a", 5, "my_value", 21.0), new Term("b", 2, "my_value", 22.0)))),
                createHistogramBucket(3000L, 0, Arrays.asList()),
                createHistogramBucket(4000L, 7, Arrays.asList(createTerms("my_field",
                        new Term("c", 4, "my_value", 41.0), new Term("b", 3, "my_value", 42.0))))
        );
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("time");
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        String json = aggToString(histogram);

        assertThat(json, equalTo("{\"time\":1000,\"my_field\":\"a\",\"my_value\":11.0,\"doc_count\":1} " +
                "{\"time\":1000,\"my_field\":\"b\",\"my_value\":12.0,\"doc_count\":2} " +
                "{\"time\":1000,\"my_field\":\"c\",\"my_value\":13.0,\"doc_count\":1} " +
                "{\"time\":2000,\"my_field\":\"a\",\"my_value\":21.0,\"doc_count\":5} " +
                "{\"time\":2000,\"my_field\":\"b\",\"my_value\":22.0,\"doc_count\":2} " +
                "{\"time\":4000,\"my_field\":\"c\",\"my_value\":41.0,\"doc_count\":4} " +
                "{\"time\":4000,\"my_field\":\"b\",\"my_value\":42.0,\"doc_count\":3}"));
    }

    public void testProcessGivenTopLevelAggIsNotHistogram() throws IOException {
        Terms terms = mock(Terms.class);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> aggToString(terms));
        assertThat(e.getMessage(), containsString("Top level aggregation should be [histogram]"));
    }

    public void testProcessGivenUnsupportedAggregationUnderHistogram() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Histogram anotherHistogram = mock(Histogram.class);
        when(anotherHistogram.getName()).thenReturn("nested-agg");
        Aggregations subAggs = createAggs(Arrays.asList(anotherHistogram));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("buckets");
        when(histogram.getBuckets()).thenReturn(Arrays.asList(histogramBucket));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> aggToString(histogram));
        assertThat(e.getMessage(), containsString("Unsupported aggregation type [nested-agg]"));
    }

    public void testProcessGivenMultipleNestedAggregations() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Terms terms1 = mock(Terms.class);
        Terms terms2 = mock(Terms.class);
        Aggregations subAggs = createAggs(Arrays.asList(terms1, terms2));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("buckets");
        when(histogram.getBuckets()).thenReturn(Arrays.asList(histogramBucket));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> aggToString(histogram));
        assertThat(e.getMessage(), containsString("Multiple nested aggregations are not supported"));
    }

    private String aggToString(Aggregation aggregation) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (AggregationToJsonProcessor processor = new AggregationToJsonProcessor(outputStream)) {
            processor.process(aggregation);
        }
        return outputStream.toString(StandardCharsets.UTF_8.name());
    }
}