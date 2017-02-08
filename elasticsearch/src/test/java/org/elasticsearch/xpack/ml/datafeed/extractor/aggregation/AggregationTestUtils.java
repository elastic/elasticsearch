/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AggregationTestUtils {

    private AggregationTestUtils() {}

    static Histogram.Bucket createHistogramBucket(long timestamp, long docCount, List<Aggregation> subAggregations) {
        Histogram.Bucket bucket = createHistogramBucket(timestamp, docCount);
        Aggregations aggs = createAggs(subAggregations);
        when(bucket.getAggregations()).thenReturn(aggs);
        return bucket;
    }

    static Aggregations createAggs(List<Aggregation> aggsList) {
        Aggregations aggs = mock(Aggregations.class);
        when(aggs.asList()).thenReturn(aggsList);
        return aggs;
    }

    static Histogram.Bucket createHistogramBucket(long timestamp, long docCount) {
        Histogram.Bucket bucket = mock(Histogram.Bucket.class);
        when(bucket.getKey()).thenReturn(timestamp);
        when(bucket.getDocCount()).thenReturn(docCount);
        return bucket;
    }

    static Histogram.Bucket createDateHistogramBucket(DateTime timestamp, long docCount) {
        Histogram.Bucket bucket = mock(Histogram.Bucket.class);
        when(bucket.getKey()).thenReturn(timestamp);
        when(bucket.getDocCount()).thenReturn(docCount);
        return bucket;
    }

    static NumericMetricsAggregation.SingleValue createSingleValue(String name, double value) {
        NumericMetricsAggregation.SingleValue singleValue = mock(NumericMetricsAggregation.SingleValue.class);
        when(singleValue.getName()).thenReturn(name);
        when(singleValue.value()).thenReturn(value);
        return singleValue;
    }

    static Terms createTerms(String name, Term... terms) {
        Terms termsAgg = mock(Terms.class);
        when(termsAgg.getName()).thenReturn(name);
        List<Terms.Bucket> buckets = new ArrayList<>();
        for (Term term: terms) {
            StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
            when(bucket.getKey()).thenReturn(term.key);
            when(bucket.getDocCount()).thenReturn(term.count);
            if (term.value != null) {
                NumericMetricsAggregation.SingleValue termValue = createSingleValue(term.valueName, term.value);
                Aggregations aggs = createAggs(Arrays.asList(termValue));
                when(bucket.getAggregations()).thenReturn(aggs);
            }
            buckets.add(bucket);
        }
        when(termsAgg.getBuckets()).thenReturn(buckets);
        return termsAgg;
    }

    static class Term {
        String key;
        long count;
        String valueName;
        Double value;

        Term(String key, long count) {
            this(key, count, null, null);
        }

        Term(String key, long count, String valueName, Double value) {
            this.key = key;
            this.count = count;
            this.valueName = valueName;
            this.value = value;
        }
    }
}
