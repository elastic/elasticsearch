/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AggregationTestUtils {

    private AggregationTestUtils() {}

    static Histogram.Bucket createHistogramBucket(long timestamp, long docCount, List<Aggregation> subAggregations) {
        Histogram.Bucket bucket = mock(Histogram.Bucket.class);
        when(bucket.getKey()).thenReturn(timestamp);
        when(bucket.getDocCount()).thenReturn(docCount);
        Aggregations aggs = createAggs(subAggregations);
        when(bucket.getAggregations()).thenReturn(aggs);
        return bucket;
    }

    static SingleBucketAggregation createSingleBucketAgg(String name, long docCount, List<Aggregation> subAggregations) {
        SingleBucketAggregation singleBucketAggregation = mock(SingleBucketAggregation.class);
        when(singleBucketAggregation.getName()).thenReturn(name);
        when(singleBucketAggregation.getDocCount()).thenReturn(docCount);
        when(singleBucketAggregation.getAggregations()).thenReturn(createAggs(subAggregations));
        return singleBucketAggregation;
    }

    static Histogram.Bucket createHistogramBucket(long timestamp, long docCount) {
        return createHistogramBucket(timestamp, docCount, Collections.emptyList());
    }

    static Aggregations createAggs(List<Aggregation> aggsList) {
        return new Aggregations(aggsList);
    }

    @SuppressWarnings("unchecked")
    static Histogram createHistogramAggregation(String name, List<Histogram.Bucket> histogramBuckets) {
        Histogram histogram = mock(Histogram.class);
        when((List<Histogram.Bucket>)histogram.getBuckets()).thenReturn(histogramBuckets);
        when(histogram.getName()).thenReturn(name);
        return histogram;
    }

    static Max createMax(String name, double value) {
        Max max = mock(Max.class);
        when(max.getName()).thenReturn(name);
        when(max.value()).thenReturn(value);
        when(max.getValue()).thenReturn(value);
        return max;
    }

    static GeoCentroid createGeoCentroid(String name, long count, double lat, double lon) {
        GeoCentroid centroid = mock(GeoCentroid.class);
        when(centroid.count()).thenReturn(count);
        when(centroid.getName()).thenReturn(name);
        GeoPoint point = count > 0 ? new GeoPoint(lat, lon) : null;
        when(centroid.centroid()).thenReturn(point);
        return centroid;
    }

    static NumericMetricsAggregation.SingleValue createSingleValue(String name, double value) {
        NumericMetricsAggregation.SingleValue singleValue = mock(NumericMetricsAggregation.SingleValue.class);
        when(singleValue.getName()).thenReturn(name);
        when(singleValue.value()).thenReturn(value);
        return singleValue;
    }

    @SuppressWarnings("unchecked")
    static Terms createTerms(String name, Term... terms) {
        Terms termsAgg = mock(Terms.class);
        when(termsAgg.getName()).thenReturn(name);
        List<Terms.Bucket> buckets = new ArrayList<>();
        for (Term term: terms) {
            StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
            when(bucket.getKey()).thenReturn(term.key);
            when(bucket.getDocCount()).thenReturn(term.count);
            List<Aggregation> numericAggs = new ArrayList<>();
            if (term.hasBuckekAggs()) {
                when(bucket.getAggregations()).thenReturn(createAggs(term.bucketAggs));
            } else {
                for (Map.Entry<String, Double> keyValue : term.values.entrySet()) {
                    numericAggs.add(createSingleValue(keyValue.getKey(), keyValue.getValue()));
                }
                if (!numericAggs.isEmpty()) {
                    Aggregations aggs = createAggs(numericAggs);
                    when(bucket.getAggregations()).thenReturn(aggs);
                }
            }
            buckets.add(bucket);
        }
        when((List<Terms.Bucket>) termsAgg.getBuckets()).thenReturn(buckets);
        return termsAgg;
    }

    static Percentiles createPercentiles(String name, double... values) {
        Percentiles percentiles = mock(Percentiles.class);
        when(percentiles.getName()).thenReturn(name);
        List<Percentile> percentileList = new ArrayList<>();
        for (double value : values) {
            Percentile percentile = mock(Percentile.class);
            when(percentile.getValue()).thenReturn(value);
            percentileList.add(percentile);
        }
        when(percentiles.iterator()).thenReturn(percentileList.iterator());
        return percentiles;
    }

    static class Term {
        String key;
        long count;
        Map<String, Double> values;
        List<Aggregation> bucketAggs;

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

        Term(String key, long count, List<Aggregation> bucketAggs) {
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
