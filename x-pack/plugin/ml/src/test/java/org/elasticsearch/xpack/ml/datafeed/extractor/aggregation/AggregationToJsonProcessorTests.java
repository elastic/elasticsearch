/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.Term;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createAggs;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createCompositeAggregation;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createCompositeBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createGeoCentroid;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramAggregation;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createMax;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createPercentiles;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createSingleBucketAgg;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createSingleValue;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationToJsonProcessorTests extends ESTestCase {

    private long keyValuePairsWritten = 0;
    private String timeField = "time";
    private boolean includeDocCount = true;
    private long startTime = 0;
    private String compositeAggValueSource = "time";

    @Before
    public void setValues() {
        compositeAggValueSource = "time";
    }

    public void testProcessGivenMultipleDateHistogramsOrComposite() {
        InternalAggregation nestedBucket;
        if (randomBoolean()) {
            List<InternalHistogram.Bucket> nestedHistogramBuckets = Arrays.asList(
                createHistogramBucket(1000L, 3, Collections.singletonList(createMax("metric1", 1200))),
                createHistogramBucket(2000L, 5, Collections.singletonList(createMax("metric1", 2800)))
            );
            nestedBucket = createHistogramAggregation("buckets", nestedHistogramBuckets);
        } else {
            List<InternalComposite.InternalBucket> nestedCompositebuckets = Arrays.asList(
                createCompositeBucket(1000L, "time", 3, Collections.singletonList(createMax("metric1", 1200)), Collections.emptyList()),
                createCompositeBucket(2000L, "time", 5, Collections.singletonList(createMax("metric1", 2800)), Collections.emptyList())
            );
            nestedBucket = createCompositeAggregation("buckets", nestedCompositebuckets);
        }

        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Arrays.asList(createMax("time", 1000L), nestedBucket))
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Sets.newHashSet("my_field"), histogramBuckets)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "More than one composite or date_histogram cannot be used in the aggregation."
                    + " [buckets] is another instance of a composite or date_histogram aggregation"
            )
        );
    }

    public void testProcessGivenMaxTimeIsMissing() {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(createHistogramBucket(1000L, 3), createHistogramBucket(2000L, 5));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Collections.emptySet(), histogramBuckets)
        );
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));

        List<InternalComposite.InternalBucket> compositeBuckets = Arrays.asList(
            createCompositeBucket(1000L, "time", 3, Collections.emptyList(), Collections.emptyList()),
            createCompositeBucket(2000L, "time", 5, Collections.emptyList(), Collections.emptyList())
        );

        e = expectThrows(IllegalArgumentException.class, () -> aggToStringComposite(Collections.emptySet(), compositeBuckets));
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));
    }

    public void testProcessGivenNonMaxTimeAgg() {
        List<InternalAggregation> aggs = Collections.singletonList(createTerms("time", new Term("a", 1), new Term("b", 2)));
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, aggs),
            createHistogramBucket(2000L, 5, aggs)
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Collections.emptySet(), histogramBuckets)
        );
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));

        List<InternalComposite.InternalBucket> compositeBuckets = Arrays.asList(
            createCompositeBucket(1000L, "time", 3, aggs, Collections.emptyList()),
            createCompositeBucket(2000L, "time", 5, aggs, Collections.emptyList())
        );

        e = expectThrows(IllegalArgumentException.class, () -> aggToStringComposite(Collections.emptySet(), compositeBuckets));
        assertThat(e.getMessage(), containsString("Missing max aggregation for time_field [time]"));
    }

    public void testProcessGivenHistogramOnly() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Collections.singletonList(createMax("timestamp", 1200))),
            createHistogramBucket(2000L, 5, Collections.singletonList(createMax("timestamp", 2800)))
        );

        timeField = "timestamp";
        String json = aggToString(Collections.emptySet(), histogramBuckets);

        assertThat(json, equalTo("{\"timestamp\":1200,\"doc_count\":3} {\"timestamp\":2800,\"doc_count\":5}"));
        assertThat(keyValuePairsWritten, equalTo(4L));
    }

    public void testProcessGivenHistogramOnlyAndNoDocCount() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Collections.singletonList(createMax("time", 1000))),
            createHistogramBucket(2000L, 5, Collections.singletonList(createMax("time", 2000)))
        );

        includeDocCount = false;
        String json = aggToString(Collections.emptySet(), histogramBuckets);

        assertThat(json, equalTo("{\"time\":1000} {\"time\":2000}"));
        assertThat(keyValuePairsWritten, equalTo(2L));
    }

    public void testProcessGivenCompositeOnly() throws IOException {
        compositeAggValueSource = "timestamp";
        List<InternalComposite.InternalBucket> compositeBuckets = Arrays.asList(
            createCompositeBucket(1000L, "timestamp", 3, Collections.singletonList(createMax("timestamp", 1200)), Collections.emptyList()),
            createCompositeBucket(2000L, "timestamp", 5, Collections.singletonList(createMax("timestamp", 2800)), Collections.emptyList())
        );

        timeField = "timestamp";
        String json = aggToStringComposite(Collections.emptySet(), compositeBuckets);

        assertThat(json, equalTo("{\"timestamp\":1200,\"doc_count\":3} {\"timestamp\":2800,\"doc_count\":5}"));
        assertThat(keyValuePairsWritten, equalTo(4L));
    }

    public void testProcessGivenCompositeOnlyAndNoDocCount() throws IOException {
        List<InternalComposite.InternalBucket> compositeBuckets = Arrays.asList(
            createCompositeBucket(1000L, "time", 3, Collections.singletonList(createMax("time", 1000)), Collections.emptyList()),
            createCompositeBucket(2000L, "time", 5, Collections.singletonList(createMax("time", 2000)), Collections.emptyList())
        );

        includeDocCount = false;
        String json = aggToStringComposite(Collections.emptySet(), compositeBuckets);

        assertThat(json, equalTo("{\"time\":1000} {\"time\":2000}"));
        assertThat(keyValuePairsWritten, equalTo(2L));
    }

    public void testProcessGivenCompositeWithDocAndTerms() throws IOException {
        compositeAggValueSource = "timestamp";
        List<InternalComposite.InternalBucket> compositeBuckets = Arrays.asList(
            createCompositeBucket(
                1000L,
                "timestamp",
                3,
                Collections.singletonList(createMax("timestamp", 1200)),
                Arrays.asList(Tuple.tuple("foo", "value1"), Tuple.tuple("bar", "value1"))
            ),
            createCompositeBucket(
                2000L,
                "timestamp",
                5,
                Collections.singletonList(createMax("timestamp", 2800)),
                Arrays.asList(Tuple.tuple("foo", "value2"), Tuple.tuple("bar", "value2"))
            )
        );

        timeField = "timestamp";
        String json = aggToStringComposite(Sets.newHashSet("foo", "bar"), compositeBuckets);

        assertThat(
            json,
            equalTo(
                "{\"bar\":\"value1\",\"foo\":\"value1\",\"timestamp\":1200,\"doc_count\":3}"
                    + " {\"bar\":\"value2\",\"foo\":\"value2\",\"timestamp\":2800,\"doc_count\":5}"
            )
        );
        assertThat(keyValuePairsWritten, equalTo(8L));
    }

    public void testProcessGivenTopLevelAggIsNotHistogram() throws IOException {
        List<InternalHistogram.Bucket> histogramABuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Arrays.asList(createMax("time", 1000), createSingleValue("my_value", 1.0))),
            createHistogramBucket(2000L, 4, Arrays.asList(createMax("time", 2000), createSingleValue("my_value", 2.0))),
            createHistogramBucket(3000L, 5, Arrays.asList(createMax("time", 3000), createSingleValue("my_value", 3.0)))
        );
        InternalHistogram histogramA = createHistogramAggregation("buckets", histogramABuckets);

        List<InternalHistogram.Bucket> histogramBBuckets = Arrays.asList(
            createHistogramBucket(1000L, 6, Arrays.asList(createMax("time", 1000), createSingleValue("my_value", 10.0))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createSingleValue("my_value", 20.0))),
            createHistogramBucket(3000L, 8, Arrays.asList(createMax("time", 3000), createSingleValue("my_value", 30.0)))
        );
        InternalHistogram histogramB = createHistogramAggregation("buckets", histogramBBuckets);

        StringTerms terms = createTerms(
            "my_field",
            new Term("A", 20, Collections.singletonList(histogramA)),
            new Term("B", 2, Collections.singletonList(histogramB))
        );

        String json = aggToString(Sets.newHashSet("my_value", "my_field"), createAggs(Collections.singletonList(terms)));
        assertThat(json, equalTo("""
            {"my_field":"A","time":1000,"my_value":1.0,"doc_count":3} \
            {"my_field":"B","time":1000,"my_value":10.0,"doc_count":6} \
            {"my_field":"A","time":2000,"my_value":2.0,"doc_count":4} \
            {"my_field":"B","time":2000,"my_value":20.0,"doc_count":7} \
            {"my_field":"A","time":3000,"my_value":3.0,"doc_count":5} \
            {"my_field":"B","time":3000,"my_value":30.0,"doc_count":8}"""));
    }

    public void testProcessGivenSingleMetricPerHistogram() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Arrays.asList(createMax("time", 1000), createSingleValue("my_value", 1.0))),
            createHistogramBucket(
                2000L,
                3,
                Arrays.asList(createMax("time", 2000), createSingleValue("my_value", Double.NEGATIVE_INFINITY))
            ),
            createHistogramBucket(3000L, 5, Arrays.asList(createMax("time", 3000), createSingleValue("my_value", 3.0)))
        );

        String json = aggToString(Sets.newHashSet("my_value"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"my_value":1.0,"doc_count":3} \
            {"time":2000,"doc_count":3} \
            {"time":3000,"my_value":3.0,"doc_count":5}"""));
    }

    public void testProcessGivenTermsPerHistogram() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(createMax("time", 1100), createTerms("my_field", new Term("a", 1), new Term("b", 2), new Term("c", 1)))
            ),
            createHistogramBucket(
                2000L,
                5,
                Arrays.asList(createMax("time", 2200), createTerms("my_field", new Term("a", 5), new Term("b", 2)))
            ),
            createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", -1))),
            createHistogramBucket(
                4000L,
                7,
                Arrays.asList(createMax("time", 4400), createTerms("my_field", new Term("c", 4), new Term("b", 3)))
            )
        );

        String json = aggToString(Sets.newHashSet("time", "my_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1100,"my_field":"a","doc_count":1} \
            {"time":1100,"my_field":"b","doc_count":2} \
            {"time":1100,"my_field":"c","doc_count":1} \
            {"time":2200,"my_field":"a","doc_count":5} \
            {"time":2200,"my_field":"b","doc_count":2} \
            {"time":4400,"my_field":"c","doc_count":4} \
            {"time":4400,"my_field":"b","doc_count":3}"""));
    }

    public void testProcessGivenSingleMetricPerSingleTermsPerHistogram() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(
                    createMax("time", 1000),
                    createTerms(
                        "my_field",
                        new Term("a", 1, "my_value", 11.0),
                        new Term("b", 2, "my_value", 12.0),
                        new Term("c", 1, "my_value", 13.0)
                    )
                )
            ),
            createHistogramBucket(
                2000L,
                5,
                Arrays.asList(
                    createMax("time", 2000),
                    createTerms("my_field", new Term("a", 5, "my_value", 21.0), new Term("b", 2, "my_value", 22.0))
                )
            ),
            createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", 3000))),
            createHistogramBucket(
                4000L,
                7,
                Arrays.asList(
                    createMax("time", 4000),
                    createTerms("my_field", new Term("c", 4, "my_value", 41.0), new Term("b", 3, "my_value", 42.0))
                )
            )
        );

        String json = aggToString(Sets.newHashSet("my_field", "my_value"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"my_field":"a","my_value":11.0,"doc_count":1} \
            {"time":1000,"my_field":"b","my_value":12.0,"doc_count":2} \
            {"time":1000,"my_field":"c","my_value":13.0,"doc_count":1} \
            {"time":2000,"my_field":"a","my_value":21.0,"doc_count":5} \
            {"time":2000,"my_field":"b","my_value":22.0,"doc_count":2} \
            {"time":4000,"my_field":"c","my_value":41.0,"doc_count":4} \
            {"time":4000,"my_field":"b","my_value":42.0,"doc_count":3}"""));
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
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(
                    createMax("time", 1000),
                    createTerms(
                        "my_field",
                        new Term("a", 1, a1NumericAggs),
                        new Term("b", 2, b1NumericAggs),
                        new Term("c", 1, c1NumericAggs)
                    )
                )
            ),
            createHistogramBucket(
                2000L,
                5,
                Arrays.asList(
                    createMax("time", 2000),
                    createTerms("my_field", new Term("a", 5, a2NumericAggs), new Term("b", 2, b2NumericAggs))
                )
            ),
            createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", 3000))),
            createHistogramBucket(
                4000L,
                7,
                Arrays.asList(
                    createMax("time", 4000),
                    createTerms("my_field", new Term("c", 4, c4NumericAggs), new Term("b", 3, b4NumericAggs))
                )
            )
        );

        includeDocCount = false;
        String json = aggToString(Sets.newHashSet("my_field", "my_value", "my_value2"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"my_field":"a","my_value":111.0,"my_value2":112.0} \
            {"time":1000,"my_field":"b","my_value2":122.0} \
            {"time":1000,"my_field":"c","my_value":131.0,"my_value2":132.0} \
            {"time":2000,"my_field":"a","my_value":211.0,"my_value2":212.0} \
            {"time":2000,"my_field":"b","my_value":221.0,"my_value2":222.0} \
            {"time":4000,"my_field":"c","my_value":411.0,"my_value2":412.0} \
            {"time":4000,"my_field":"b","my_value":421.0,"my_value2":422.0}"""));
    }

    public void testProcessGivenUnsupportedAggregationUnderHistogram() {
        InternalAggregation anotherHistogram = mock(InternalAggregation.class);
        when(anotherHistogram.getName()).thenReturn("nested-agg");
        InternalHistogram.Bucket histogramBucket = createHistogramBucket(
            1000L,
            2,
            Arrays.asList(createMax("time", 1000), anotherHistogram)
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Sets.newHashSet("nested-agg"), histogramBucket)
        );
        assertThat(e.getMessage(), containsString("Unsupported aggregation type [nested-agg]"));
    }

    public void testProcessGivenMultipleBucketAggregations() {
        StringTerms terms1 = mock(StringTerms.class);
        when(terms1.getName()).thenReturn("terms_1");
        StringTerms terms2 = mock(StringTerms.class);
        when(terms2.getName()).thenReturn("terms_2");
        InternalHistogram.Bucket histogramBucket = createHistogramBucket(1000L, 2, Arrays.asList(createMax("time", 1000), terms1, terms2));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Sets.newHashSet("terms_1", "terms_2"), histogramBucket)
        );
        assertThat(e.getMessage(), containsString("Multiple bucket aggregations at the same level are not supported"));
    }

    public void testProcessGivenMixedBucketAndLeafAggregationsAtSameLevel_BucketFirst() throws IOException {
        StringTerms terms = createTerms("terms", new Term("a", 1), new Term("b", 2));
        Max maxAgg = createMax("max_value", 1200);
        InternalHistogram.Bucket histogramBucket = createHistogramBucket(1000L, 2, Arrays.asList(terms, createMax("time", 1000), maxAgg));

        String json = aggToString(Sets.newHashSet("terms", "max_value"), histogramBucket);
        assertThat(json, equalTo("""
            {"time":1000,"max_value":1200.0,"terms":"a","doc_count":1} \
            {"time":1000,"max_value":1200.0,"terms":"b","doc_count":2}"""));
    }

    public void testProcessGivenMixedBucketAndLeafAggregationsAtSameLevel_LeafFirst() throws IOException {
        Max maxAgg = createMax("max_value", 1200);
        StringTerms terms = createTerms("terms", new Term("a", 1), new Term("b", 2));
        InternalHistogram.Bucket histogramBucket = createHistogramBucket(1000L, 2, Arrays.asList(createMax("time", 1000), maxAgg, terms));

        String json = aggToString(Sets.newHashSet("terms", "max_value"), histogramBucket);
        assertThat(json, equalTo("""
            {"time":1000,"max_value":1200.0,"terms":"a","doc_count":1} \
            {"time":1000,"max_value":1200.0,"terms":"b","doc_count":2}"""));
    }

    public void testProcessGivenBucketAndLeafAggregationsButBucketNotInFields() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(
                    createMax("time", 1100),
                    createMax("my_value", 1),
                    createTerms("my_field", new Term("a", 1), new Term("b", 2), new Term("c", 1))
                )
            ),
            createHistogramBucket(
                2000L,
                5,
                Arrays.asList(
                    createMax("time", 2200),
                    createMax("my_value", 2),
                    createTerms("my_field", new Term("a", 5), new Term("b", 2))
                )
            ),
            createHistogramBucket(3000L, 0, Collections.singletonList(createMax("time", -1))),
            createHistogramBucket(
                4000L,
                7,
                Arrays.asList(
                    createMax("time", 4400),
                    createMax("my_value", 4),
                    createTerms("my_field", new Term("c", 4), new Term("b", 3))
                )
            )
        );

        String json = aggToString(Sets.newHashSet("time", "my_value"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1100,"my_value":1.0,"doc_count":4} \
            {"time":2200,"my_value":2.0,"doc_count":5} \
            {"time":4400,"my_value":4.0,"doc_count":7}"""));
    }

    public void testProcessGivenEmptyPercentiles() throws IOException {
        var histogramBuckets = List.of(createHistogramBucket(1000L, 4, List.of(createMax("time", 1000), createPercentiles("my_field"))));

        var json = aggToString(Sets.newHashSet("my_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"doc_count":4}"""));
    }

    public void testProcessGivenSinglePercentilesPerHistogram() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 4, Arrays.asList(createMax("time", 1000), createPercentiles("my_field", 1.0))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createPercentiles("my_field", 2.0))),
            createHistogramBucket(
                3000L,
                10,
                Arrays.asList(createMax("time", 3000), createPercentiles("my_field", Double.NEGATIVE_INFINITY))
            ),
            createHistogramBucket(4000L, 14, Arrays.asList(createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        String json = aggToString(Sets.newHashSet("my_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"my_field":1.0,"doc_count":4} \
            {"time":2000,"my_field":2.0,"doc_count":7} \
            {"time":3000,"doc_count":10} \
            {"time":4000,"my_field":4.0,"doc_count":14}"""));
    }

    public void testProcessGivenMultiplePercentilesPerHistogram() {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 4, Arrays.asList(createMax("time", 1000), createPercentiles("my_field", 1.0))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createPercentiles("my_field", 2.0, 5.0))),
            createHistogramBucket(3000L, 10, Arrays.asList(createMax("time", 3000), createPercentiles("my_field", 3.0))),
            createHistogramBucket(4000L, 14, Arrays.asList(createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> aggToString(Sets.newHashSet("my_field"), histogramBuckets)
        );
        assertThat(e.getMessage(), containsString("Multi-percentile aggregation [my_field] is not supported"));
    }

    @SuppressWarnings("unchecked")
    public void testBucketAggContainsRequiredAgg() {
        Set<String> fields = new HashSet<>();
        fields.add("foo");
        AggregationToJsonProcessor processor = new AggregationToJsonProcessor("time", fields, false, 0L, null);

        Terms termsAgg = mock(Terms.class);
        when(termsAgg.getBuckets()).thenReturn(Collections.emptyList());
        when(termsAgg.getName()).thenReturn("foo");
        assertTrue(processor.bucketAggContainsRequiredAgg(termsAgg));

        when(termsAgg.getName()).thenReturn("bar");
        assertFalse(processor.bucketAggContainsRequiredAgg(termsAgg));

        StringTerms nestedTermsAgg = mock(StringTerms.class);
        when(nestedTermsAgg.getBuckets()).thenReturn(Collections.emptyList());
        when(nestedTermsAgg.getName()).thenReturn("nested_terms");

        StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
        when(bucket.getAggregations()).thenReturn(InternalAggregations.from(Collections.singletonList(nestedTermsAgg)));
        when((List<Terms.Bucket>) termsAgg.getBuckets()).thenReturn(Collections.singletonList(bucket));
        assertFalse(processor.bucketAggContainsRequiredAgg(termsAgg));

        Max max = mock(Max.class);
        when(max.getName()).thenReturn("foo");
        StringTerms.Bucket nestedTermsBucket = mock(StringTerms.Bucket.class);
        when(nestedTermsBucket.getAggregations()).thenReturn(InternalAggregations.from(Collections.singletonList(max)));
        when(nestedTermsAgg.getBuckets()).thenReturn(Collections.singletonList(nestedTermsBucket));
        assertTrue(processor.bucketAggContainsRequiredAgg(termsAgg));
    }

    public void testBucketBeforeStartIsPruned() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 4, Arrays.asList(createMax("time", 1000), createPercentiles("my_field", 1.0))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createPercentiles("my_field", 2.0))),
            createHistogramBucket(3000L, 10, Arrays.asList(createMax("time", 3000), createPercentiles("my_field", 3.0))),
            createHistogramBucket(4000L, 14, Arrays.asList(createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        startTime = 2000;
        String json = aggToString(Sets.newHashSet("my_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":2000,"my_field":2.0,"doc_count":7} \
            {"time":3000,"my_field":3.0,"doc_count":10} \
            {"time":4000,"my_field":4.0,"doc_count":14}"""));
    }

    public void testBucketsBeforeStartArePruned() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 4, Arrays.asList(createMax("time", 1000), createPercentiles("my_field", 1.0))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createPercentiles("my_field", 2.0))),
            createHistogramBucket(3000L, 10, Arrays.asList(createMax("time", 3000), createPercentiles("my_field", 3.0))),
            createHistogramBucket(4000L, 14, Arrays.asList(createMax("time", 4000), createPercentiles("my_field", 4.0)))
        );

        startTime = 3000;
        String json = aggToString(Sets.newHashSet("my_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":3000,"my_field":3.0,"doc_count":10} \
            {"time":4000,"my_field":4.0,"doc_count":14}"""));
    }

    public void testSingleBucketAgg() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(
                    createMax("time", 1000),
                    createSingleBucketAgg("agg1", 3, Collections.singletonList(createMax("field1", 5.0))),
                    createSingleBucketAgg("agg2", 1, Collections.singletonList(createMax("field2", 3.0)))
                )
            ),
            createHistogramBucket(
                2000L,
                7,
                Arrays.asList(
                    createMax("time", 2000),
                    createSingleBucketAgg("agg2", 3, Collections.singletonList(createMax("field2", 1.0))),
                    createSingleBucketAgg("agg1", 4, Collections.singletonList(createMax("field1", 7.0)))
                )
            )
        );

        String json = aggToString(Sets.newHashSet("field1", "field2"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"field1":5.0,"field2":3.0,"doc_count":4} \
            {"time":2000,"field2":1.0,"field1":7.0,"doc_count":7}"""));
    }

    public void testSingleBucketAgg_failureWithSubMultiBucket() {
        List<InternalHistogram.Bucket> histogramBuckets = Collections.singletonList(
            createHistogramBucket(
                1000L,
                4,
                Arrays.asList(
                    createMax("time", 1000),
                    createSingleBucketAgg(
                        "agg1",
                        3,
                        Arrays.asList(createHistogramAggregation("histo", Collections.emptyList()), createMax("field1", 5.0))
                    ),
                    createSingleBucketAgg(
                        "agg2",
                        1,
                        Arrays.asList(createHistogramAggregation("histo", Collections.emptyList()), createMax("field1", 3.0))
                    )
                )
            )
        );

        expectThrows(IllegalArgumentException.class, () -> aggToString(Sets.newHashSet("my_field"), histogramBuckets));
    }

    public void testGeoCentroidAgg() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 4, Arrays.asList(createMax("time", 1000), createGeoCentroid("geo_field", 4, 92.1, 93.1))),
            createHistogramBucket(2000L, 7, Arrays.asList(createMax("time", 2000), createGeoCentroid("geo_field", 0, -1, -1)))
        );
        String json = aggToString(Sets.newHashSet("geo_field"), histogramBuckets);

        assertThat(json, equalTo("""
            {"time":1000,"geo_field":"92.1,93.1","doc_count":4} \
            {"time":2000,"doc_count":7}"""));
    }

    private String aggToString(Set<String> fields, InternalHistogram.Bucket bucket) throws IOException {
        return aggToString(fields, Collections.singletonList(bucket));
    }

    private String aggToString(Set<String> fields, List<InternalHistogram.Bucket> buckets) throws IOException {
        InternalHistogram histogram = createHistogramAggregation("buckets", buckets);
        return aggToString(fields, createAggs(Collections.singletonList(histogram)));
    }

    private String aggToString(Set<String> fields, InternalAggregations aggregations) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        AggregationToJsonProcessor processor = new AggregationToJsonProcessor(
            timeField,
            fields,
            includeDocCount,
            startTime,
            compositeAggValueSource
        );
        processor.process(aggregations);
        processor.writeAllDocsCancellable(_timestamp -> false, outputStream);
        keyValuePairsWritten = processor.getKeyValueCount();
        return outputStream.toString(StandardCharsets.UTF_8);
    }

    private String aggToStringComposite(Set<String> fields, List<InternalComposite.InternalBucket> buckets) throws IOException {
        InternalComposite compositeAggregation = createCompositeAggregation("buckets", buckets);
        return aggToString(fields, createAggs(Collections.singletonList(compositeAggregation)));
    }

}
