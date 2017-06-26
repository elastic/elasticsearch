/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.Term;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createAggs;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createMax;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createPercentiles;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createSingleValue;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationToJsonProcessorTests extends ESTestCase {

    private long keyValuePairsWritten = 0;

    public void testProcessGivenMaxTimeIsMissing() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3),
                createHistogramBucket(2000L, 5)
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Collections.emptySet(), histogramBuckets));
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));
    }

    public void testProcessGivenNonMaxTimeAgg() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Collections.singletonList(createTerms("time"))),
                createHistogramBucket(2000L, 5, Collections.singletonList(createTerms("time")))
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Collections.emptySet(), histogramBuckets));
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));
    }

    public void testProcessGivenHistogramOnly() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Collections.singletonList(createMax("timestamp", 1200))),
                createHistogramBucket(2000L, 5, Collections.singletonList(createMax("timestamp", 2800)))
        );

        String json = aggToString("timestamp", Collections.emptySet(), histogramBuckets);

        assertThat(json, equalTo("{\"timestamp\":1200,\"doc_count\":3} {\"timestamp\":2800,\"doc_count\":5}"));
        assertThat(keyValuePairsWritten, equalTo(4L));
    }

    public void testProcessGivenHistogramOnlyAndNoDocCount() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Collections.singletonList(createMax("time", 1000))),
                createHistogramBucket(2000L, 5, Collections.singletonList(createMax("time", 2000)))
        );

        String json = aggToString("time", Collections.emptySet(), false, histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000} {\"time\":2000}"));
        assertThat(keyValuePairsWritten, equalTo(2L));
    }

    public void testProcessGivenSingleMetricPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Arrays.asList(
                        createMax("time", 1000), createSingleValue("my_value", 1.0))),
                createHistogramBucket(2000L, 3, Arrays.asList(
                        createMax("time", 2000), createSingleValue("my_value", Double.NEGATIVE_INFINITY))),
                createHistogramBucket(3000L, 5, Arrays.asList(
                        createMax("time", 3000), createSingleValue("my_value", 3.0)))
        );

        String json = aggToString("time", Sets.newHashSet("my_value"), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000,\"my_value\":1.0,\"doc_count\":3} {\"time\":3000,\"my_value\":3.0,\"doc_count\":5}"));
    }

    public void testProcessGivenTermsPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1100),
                        createTerms("my_field", new Term("a", 1), new Term("b", 2), new Term("c", 1)))),
                createHistogramBucket(2000L, 5, Arrays.asList(
                        createMax("time", 2200),
                        createTerms("my_field", new Term("a", 5), new Term("b", 2)))),
                createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", -1))),
                createHistogramBucket(4000L, 7, Arrays.asList(
                        createMax("time", 4400),
                        createTerms("my_field", new Term("c", 4), new Term("b", 3))))
        );

        String json = aggToString("time", Sets.newHashSet("time", "my_field"), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1100,\"my_field\":\"a\",\"doc_count\":1} " +
                "{\"time\":1100,\"my_field\":\"b\",\"doc_count\":2} " +
                "{\"time\":1100,\"my_field\":\"c\",\"doc_count\":1} " +
                "{\"time\":2200,\"my_field\":\"a\",\"doc_count\":5} " +
                "{\"time\":2200,\"my_field\":\"b\",\"doc_count\":2} " +
                "{\"time\":4400,\"my_field\":\"c\",\"doc_count\":4} " +
                "{\"time\":4400,\"my_field\":\"b\",\"doc_count\":3}"));
    }

    public void testProcessGivenSingleMetricPerSingleTermsPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1000),
                        createTerms("my_field", new Term("a", 1, "my_value", 11.0),
                                new Term("b", 2, "my_value", 12.0), new Term("c", 1, "my_value", 13.0)))),
                createHistogramBucket(2000L, 5, Arrays.asList(
                        createMax("time", 2000),
                        createTerms("my_field", new Term("a", 5, "my_value", 21.0), new Term("b", 2, "my_value", 22.0)))),
                createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", 3000))),
                createHistogramBucket(4000L, 7, Arrays.asList(
                        createMax("time", 4000),
                        createTerms("my_field", new Term("c", 4, "my_value", 41.0), new Term("b", 3, "my_value", 42.0))))
        );

        String json = aggToString("time", Sets.newHashSet("my_field", "my_value"), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000,\"my_field\":\"a\",\"my_value\":11.0,\"doc_count\":1} " +
                "{\"time\":1000,\"my_field\":\"b\",\"my_value\":12.0,\"doc_count\":2} " +
                "{\"time\":1000,\"my_field\":\"c\",\"my_value\":13.0,\"doc_count\":1} " +
                "{\"time\":2000,\"my_field\":\"a\",\"my_value\":21.0,\"doc_count\":5} " +
                "{\"time\":2000,\"my_field\":\"b\",\"my_value\":22.0,\"doc_count\":2} " +
                "{\"time\":4000,\"my_field\":\"c\",\"my_value\":41.0,\"doc_count\":4} " +
                "{\"time\":4000,\"my_field\":\"b\",\"my_value\":42.0,\"doc_count\":3}"));
    }

    public void testProcessGivenMultipleSingleMetricPerSingleTermsPerHistogram() throws IOException {
        Map<String, Double> a1NumericAggs = new LinkedHashMap<>();
        a1NumericAggs.put("my_value", 111.0);
        a1NumericAggs.put("my_value2", 112.0);
        Map<String, Double> b1NumericAggs = new LinkedHashMap<>();
        b1NumericAggs.put("my_value", Double.POSITIVE_INFINITY);
        b1NumericAggs.put("my_value2", 122.0);
        Map<String, Double> c1NumericAggs = new LinkedHashMap<>();
        c1NumericAggs.put("my_value", 131.0);
        c1NumericAggs.put("my_value2", 132.0);
        Map<String, Double> a2NumericAggs = new LinkedHashMap<>();
        a2NumericAggs.put("my_value", 211.0);
        a2NumericAggs.put("my_value2", 212.0);
        Map<String, Double> b2NumericAggs = new LinkedHashMap<>();
        b2NumericAggs.put("my_value", 221.0);
        b2NumericAggs.put("my_value2", 222.0);
        Map<String, Double> c4NumericAggs = new LinkedHashMap<>();
        c4NumericAggs.put("my_value", 411.0);
        c4NumericAggs.put("my_value2", 412.0);
        Map<String, Double> b4NumericAggs = new LinkedHashMap<>();
        b4NumericAggs.put("my_value", 421.0);
        b4NumericAggs.put("my_value2", 422.0);
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1000),
                        createTerms("my_field", new Term("a", 1, a1NumericAggs),
                                new Term("b", 2, b1NumericAggs), new Term("c", 1, c1NumericAggs)))),
                createHistogramBucket(2000L, 5, Arrays.asList(
                        createMax("time", 2000),
                        createTerms("my_field", new Term("a", 5, a2NumericAggs), new Term("b", 2, b2NumericAggs)))),
                createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", 3000))),
                createHistogramBucket(4000L, 7, Arrays.asList(
                        createMax("time", 4000),
                        createTerms("my_field", new Term("c", 4, c4NumericAggs), new Term("b", 3, b4NumericAggs))))
        );

        String json = aggToString("time", Sets.newHashSet("my_field", "my_value", "my_value2"), false, histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000,\"my_field\":\"a\",\"my_value\":111.0,\"my_value2\":112.0} " +
                "{\"time\":1000,\"my_field\":\"b\",\"my_value2\":122.0} " +
                "{\"time\":1000,\"my_field\":\"c\",\"my_value\":131.0,\"my_value2\":132.0} " +
                "{\"time\":2000,\"my_field\":\"a\",\"my_value\":211.0,\"my_value2\":212.0} " +
                "{\"time\":2000,\"my_field\":\"b\",\"my_value\":221.0,\"my_value2\":222.0} " +
                "{\"time\":4000,\"my_field\":\"c\",\"my_value\":411.0,\"my_value2\":412.0} " +
                "{\"time\":4000,\"my_field\":\"b\",\"my_value\":421.0,\"my_value2\":422.0}"));
    }

    public void testProcessGivenUnsupportedAggregationUnderHistogram() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Aggregation anotherHistogram = mock(Aggregation.class);
        when(anotherHistogram.getName()).thenReturn("nested-agg");
        Aggregations subAggs = createAggs(Arrays.asList(createMax("time", 1000), anotherHistogram));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Sets.newHashSet("nested-agg"), histogramBucket));
        assertThat(e.getMessage(), containsString("Unsupported aggregation type [nested-agg]"));
    }

    public void testProcessGivenMultipleBucketAggregations() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Terms terms1 = mock(Terms.class);
        when(terms1.getName()).thenReturn("terms_1");
        Terms terms2 = mock(Terms.class);
        when(terms2.getName()).thenReturn("terms_2");
        Aggregations subAggs = createAggs(Arrays.asList(createMax("time", 1000), terms1, terms2));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Sets.newHashSet("terms_1", "terms_2"), histogramBucket));
        assertThat(e.getMessage(), containsString("Multiple bucket aggregations at the same level are not supported"));
    }

    public void testProcessGivenMixedBucketAndLeafAggregationsAtSameLevel_BucketFirst() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Terms terms = mock(Terms.class);
        when(terms.getName()).thenReturn("terms");
        Max maxAgg = createMax("max_value", 1200);
        Aggregations subAggs = createAggs(Arrays.asList(createMax("time", 1000), terms, maxAgg));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Sets.newHashSet("terms", "max_value"), histogramBucket));
        assertThat(e.getMessage(), containsString("Mixing bucket and leaf aggregations at the same level is not supported"));
    }

    public void testProcessGivenMixedBucketAndLeafAggregationsAtSameLevel_LeafFirst() throws IOException {
        Histogram.Bucket histogramBucket = createHistogramBucket(1000L, 2);
        Max maxAgg = createMax("max_value", 1200);
        Terms terms = mock(Terms.class);
        when(terms.getName()).thenReturn("terms");
        Aggregations subAggs = createAggs(Arrays.asList(createMax("time", 1000), maxAgg, terms));
        when(histogramBucket.getAggregations()).thenReturn(subAggs);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Sets.newHashSet("terms", "max_value"), histogramBucket));
        assertThat(e.getMessage(), containsString("Mixing bucket and leaf aggregations at the same level is not supported"));
    }

    public void testProcessGivenBucketAndLeafAggregationsButBucketNotInFields() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1100),
                        createMax("my_value", 1),
                        createTerms("my_field", new Term("a", 1), new Term("b", 2), new Term("c", 1)))),
                createHistogramBucket(2000L, 5, Arrays.asList(
                        createMax("time", 2200),
                        createMax("my_value", 2),
                        createTerms("my_field", new Term("a", 5), new Term("b", 2)))),
                createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", -1))),
                createHistogramBucket(4000L, 7, Arrays.asList(
                        createMax("time", 4400),
                        createMax("my_value", 4),
                        createTerms("my_field", new Term("c", 4), new Term("b", 3))))
        );

        String json = aggToString("time", Sets.newHashSet("time", "my_value"), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1100,\"my_value\":1.0,\"doc_count\":4} " +
                "{\"time\":2200,\"my_value\":2.0,\"doc_count\":5} " +
                "{\"time\":4400,\"my_value\":4.0,\"doc_count\":7}"));
    }

    public void testProcessGivenSinglePercentilesPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1000), createPercentiles("my_field", 1.0))),
                createHistogramBucket(2000L, 7, Arrays.asList(
                        createMax("time", 2000), createPercentiles("my_field", 2.0))),
                createHistogramBucket(3000L, 10, Arrays.asList(
                        createMax("time", 3000), createPercentiles("my_field", Double.NEGATIVE_INFINITY))),
                createHistogramBucket(4000L, 14, Arrays.asList(
                        createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        String json = aggToString("time", Sets.newHashSet("my_field"), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000,\"my_field\":1.0,\"doc_count\":4} " +
                "{\"time\":2000,\"my_field\":2.0,\"doc_count\":7} " +
                "{\"time\":4000,\"my_field\":4.0,\"doc_count\":14}"));
    }

    public void testProcessGivenMultiplePercentilesPerHistogram() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 4, Arrays.asList(
                        createMax("time", 1000), createPercentiles("my_field", 1.0))),
                createHistogramBucket(2000L, 7, Arrays.asList(
                        createMax("time", 2000), createPercentiles("my_field", 2.0, 5.0))),
                createHistogramBucket(3000L, 10, Arrays.asList(
                        createMax("time", 3000), createPercentiles("my_field", 3.0))),
                createHistogramBucket(4000L, 14, Arrays.asList(
                        createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> aggToString("time", Sets.newHashSet("my_field"), histogramBuckets));
        assertThat(e.getMessage(), containsString("Multi-percentile aggregation [my_field] is not supported"));
    }

    private String aggToString(String timeField, Set<String> fields, Histogram.Bucket bucket) throws IOException {
        return aggToString(timeField, fields, true, Collections.singletonList(bucket));
    }

    private String aggToString(String timeField, Set<String> fields, List<Histogram.Bucket> buckets) throws IOException {
        return aggToString(timeField, fields, true, buckets);
    }

    private String aggToString(String timeField, Set<String> fields, boolean includeDocCount, List<Histogram.Bucket> buckets)
            throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (AggregationToJsonProcessor processor = new AggregationToJsonProcessor(timeField, fields, includeDocCount, outputStream)) {
            for (Histogram.Bucket bucket : buckets) {
                processor.process(bucket);
            }
            keyValuePairsWritten = processor.getKeyValueCount();
        }
        return outputStream.toString(StandardCharsets.UTF_8.name());
    }
}