/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MockAggregations {

    public static StringTerms mockTerms(String name) {
        return mockTerms(name, Collections.emptyList(), 0);
    }

    public static StringTerms mockTerms(String name, List<StringTerms.Bucket> buckets, long sumOfOtherDocCounts) {
        StringTerms agg = mock(StringTerms.class);
        when(agg.getName()).thenReturn(name);
        doReturn(buckets).when(agg).getBuckets();
        when(agg.getSumOfOtherDocCounts()).thenReturn(sumOfOtherDocCounts);
        return agg;
    }

    public static StringTerms.Bucket mockTermsBucket(String key, InternalAggregations subAggs) {
        StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    public static InternalFilters mockFilters(String name) {
        return mockFilters(name, Collections.emptyList());
    }

    public static InternalFilters mockFilters(String name, List<Filters.Bucket> buckets) {
        InternalFilters agg = mock(InternalFilters.class);
        when(agg.getName()).thenReturn(name);
        doReturn(buckets).when(agg).getBuckets();
        return agg;
    }

    public static InternalFilters.InternalBucket mockFiltersBucket(String key, long docCount, InternalAggregations subAggs) {
        InternalFilters.InternalBucket bucket = mockFiltersBucket(key, docCount);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    public static InternalFilters.InternalBucket mockFiltersBucket(String key, long docCount) {
        InternalFilters.InternalBucket bucket = mock(InternalFilters.InternalBucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getDocCount()).thenReturn(docCount);
        return bucket;
    }

    public static InternalFilter mockFilter(String name, long docCount) {
        InternalFilter agg = mock(InternalFilter.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getDocCount()).thenReturn(docCount);
        return agg;
    }

    public static InternalNumericMetricsAggregation.SingleValue mockSingleValue(String name, double value) {
        InternalNumericMetricsAggregation.SingleValue agg = mock(InternalNumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    public static InternalCardinality mockCardinality(String name, long value) {
        InternalCardinality agg = mock(InternalCardinality.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getValue()).thenReturn(value);
        return agg;
    }

    public static InternalExtendedStats mockExtendedStats(String name, double variance, long count) {
        InternalExtendedStats agg = mock(InternalExtendedStats.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getVariance()).thenReturn(variance);
        when(agg.getCount()).thenReturn(count);
        return agg;
    }
}
