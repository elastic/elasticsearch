/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Percentile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AggregationTestUtils {

    private AggregationTestUtils() {}

    static InternalHistogram.Bucket createHistogramBucket(long timestamp, long docCount, List<InternalAggregation> subAggregations) {
        return new InternalHistogram.Bucket(timestamp, docCount, DocValueFormat.RAW, createAggs(subAggregations));
    }

    static InternalComposite.InternalBucket createCompositeBucket(
        long timestamp,
        String dateValueSource,
        long docCount,
        List<InternalAggregation> subAggregations,
        List<Tuple<String, String>> termValues
    ) {
        InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
        when(bucket.getDocCount()).thenReturn(docCount);
        InternalAggregations aggs = createAggs(subAggregations);
        when(bucket.getAggregations()).thenReturn(aggs);
        Map<String, Object> bucketKey = new HashMap<>();
        bucketKey.put(dateValueSource, timestamp);
        for (Tuple<String, String> termValue : termValues) {
            bucketKey.put(termValue.v1(), termValue.v2());
        }
        when(bucket.getKey()).thenReturn(bucketKey);
        return bucket;
    }

    static InternalSingleBucketAggregation createSingleBucketAgg(String name, long docCount, List<InternalAggregation> subAggregations) {
        InternalSingleBucketAggregation singleBucketAggregation = mock(InternalSingleBucketAggregation.class);
        when(singleBucketAggregation.getName()).thenReturn(name);
        when(singleBucketAggregation.getDocCount()).thenReturn(docCount);
        when(singleBucketAggregation.getAggregations()).thenReturn(createAggs(subAggregations));
        return singleBucketAggregation;
    }

    static InternalHistogram.Bucket createHistogramBucket(long timestamp, long docCount) {
        return createHistogramBucket(timestamp, docCount, Collections.emptyList());
    }

    static InternalAggregations createAggs(List<InternalAggregation> aggsList) {
        return InternalAggregations.from(aggsList);
    }

    @SuppressWarnings("unchecked")
    static InternalHistogram createHistogramAggregation(String name, List<InternalHistogram.Bucket> histogramBuckets) {
        InternalHistogram histogram = mock(InternalHistogram.class);
        when(histogram.getBuckets()).thenReturn(histogramBuckets);
        when(histogram.getName()).thenReturn(name);
        return histogram;
    }

    @SuppressWarnings("unchecked")
    static InternalComposite createCompositeAggregation(String name, List<InternalComposite.InternalBucket> buckets) {
        InternalComposite compositeAggregation = mock(InternalComposite.class);
        when(compositeAggregation.getBuckets()).thenReturn(buckets);
        when(compositeAggregation.getName()).thenReturn(name);
        return compositeAggregation;

    }

    static Max createMax(String name, double value) {
        return new Max(name, value, DocValueFormat.RAW, null);
    }

    static InternalAvg createAvg(String name, double value) {
        InternalAvg avg = mock(InternalAvg.class);
        when(avg.getName()).thenReturn(name);
        when(avg.value()).thenReturn(value);
        when(avg.getValue()).thenReturn(value);
        return avg;
    }

    static InternalGeoCentroid createGeoCentroid(String name, long count, double lat, double lon) {
        InternalGeoCentroid centroid = mock(InternalGeoCentroid.class);
        when(centroid.count()).thenReturn(count);
        when(centroid.getName()).thenReturn(name);
        GeoPoint point = count > 0 ? new GeoPoint(lat, lon) : null;
        when(centroid.centroid()).thenReturn(point);
        return centroid;
    }

    static InternalNumericMetricsAggregation.SingleValue createSingleValue(String name, double value) {
        InternalNumericMetricsAggregation.SingleValue singleValue = mock(InternalNumericMetricsAggregation.SingleValue.class);
        when(singleValue.getName()).thenReturn(name);
        when(singleValue.value()).thenReturn(value);
        return singleValue;
    }

    @SuppressWarnings("unchecked")
    static StringTerms createTerms(String name, Term... terms) {
        StringTerms termsAgg = mock(StringTerms.class);
        when(termsAgg.getName()).thenReturn(name);
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (Term term : terms) {
            StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
            when(bucket.getKey()).thenReturn(term.key);
            when(bucket.getDocCount()).thenReturn(term.count);
            List<InternalAggregation> numericAggs = new ArrayList<>();
            if (term.hasBuckekAggs()) {
                when(bucket.getAggregations()).thenReturn(createAggs(term.bucketAggs));
            } else {
                for (Map.Entry<String, Double> keyValue : term.values.entrySet()) {
                    numericAggs.add(createSingleValue(keyValue.getKey(), keyValue.getValue()));
                }
                if (numericAggs.isEmpty() == false) {
                    InternalAggregations aggs = createAggs(numericAggs);
                    when(bucket.getAggregations()).thenReturn(aggs);
                }
            }
            buckets.add(bucket);
        }
        when(termsAgg.getBuckets()).thenReturn(buckets);
        return termsAgg;
    }

    static InternalTDigestPercentiles createPercentiles(String name, double... values) {
        InternalTDigestPercentiles percentiles = mock(InternalTDigestPercentiles.class);
        when(percentiles.getName()).thenReturn(name);
        List<Percentile> percentileList = new ArrayList<>();
        for (double value : values) {
            percentileList.add(new Percentile(0.0, value));
        }
        when(percentiles.iterator()).thenReturn(percentileList.iterator());
        return percentiles;
    }

    static class Term {
        String key;
        long count;
        Map<String, Double> values;
        List<InternalAggregation> bucketAggs;

        Term(String key, long count) {
            this(key, count, Collections.emptyMap());
        }

        Term(String key, long count, String valueName, Double value) {
            this(key, count, newKeyValue(valueName, value));
        }

        Term(String key, long count, Map<String, Double> values) {
            this.key = key;
            this.count = count;
            this.values = values;
        }

        Term(String key, long count, List<InternalAggregation> bucketAggs) {
            this(key, count);
            this.bucketAggs = bucketAggs;
        }

        private boolean hasBuckekAggs() {
            return bucketAggs != null;
        }

        private static Map<String, Double> newKeyValue(String key, Double value) {
            Map<String, Double> keyValue = new HashMap<>();
            keyValue.put(key, value);
            return keyValue;
        }
    }
}
