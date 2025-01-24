/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class CumulativeSumPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;

    CumulativeSumPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> histo = (InternalMultiBucketAggregation<
                ? extends InternalMultiBucketAggregation,
                ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;

        if (aggregation instanceof LongTerms || aggregation instanceof DoubleTerms  || aggregation instanceof StringTerms) {
            return reduceTermsAgg(histo, reduceContext);
        }

        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;
        List<Bucket> newBuckets = new ArrayList<>(buckets.size());
        double sum = 0;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], GapPolicy.INSERT_ZEROS);

            // Only increment the sum if it's a finite value, otherwise "increment by zero" is correct
            if (thisBucketValue != null && thisBucketValue.isInfinite() == false && thisBucketValue.isNaN() == false) {
                sum += thisBucketValue;
            }

            List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                .collect(Collectors.toCollection(ArrayList::new));
            aggs.add(new InternalSimpleValue(name(), sum, formatter, metadata()));
            Bucket newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
            newBuckets.add(newBucket);
        }
        return factory.createAggregation(newBuckets);
    }



    public InternalAggregation createBucket(InternalAggregation aggregation, Map<Integer, List<InternalAggregation>> mapAggregations, List<? extends InternalMultiBucketAggregation.InternalBucket> allBuckets) {
        List<Bucket> completeBuckets = new ArrayList<>();
        Bucket longBucket = LongTerms.Bucket;
        Bucket doubleBucket = DoubleTerms.Bucket;
        Bucket stringBucket = StringTerms.Bucket;
        var longFactory = (LongTerms) aggregation;
        var doubleFactory = (DoubleTerms) aggregation;
        var stringFactory = (StringTerms) aggregation;

        if (aggregation instanceof LongTerms) {
            longFactory = (LongTerms) aggregation;
            for (Map.Entry<Integer, List<InternalAggregation>> entry : mapAggregations.entrySet()) {
                completeBuckets.add((Bucket) longFactory.createBucket(longBucket, entry.getValue(), allBuckets.get(entry.getKey())));
            }
            return longFactory.create(completeBuckets);
        } else if (aggregation instanceof DoubleTerms) {
            doubleFactory = (DoubleTerms) aggregation;
            for (Map.Entry<Integer, List<InternalAggregation>> entry : mapAggregations.entrySet()) {
                completeBuckets.add((Bucket) doubleFactory.createBucket(doubleBucket, entry.getValue(), allBuckets.get(entry.getKey())));
            }
            return doubleFactory.create(completeBuckets);
        }
        stringFactory = (StringTerms) aggregation;
        for (Map.Entry<Integer, List<InternalAggregation>> entry : mapAggregations.entrySet()) {
            completeBuckets.add((Bucket) stringFactory.createBucket(stringBucket, entry.getValue(), allBuckets.get(entry.getKey())));
        }
        return longFactory.create(completeBuckets);
    }

    public InternalAggregation reduceTermsAgg(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> terms = (InternalMultiBucketAggregation< ? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> allBuckets = terms.getBuckets();

        double sum = 0.0;

        Map<Integer, List<InternalAggregation>> mapAggregations = new HashMap<Integer, List<InternalAggregation>>();

        for (int i = 0; i < allBuckets.size(); ++i) {
            InternalMultiBucketAggregation.InternalBucket bucket = allBuckets.get(i);
            Double thisBucketValue = resolveBucketValue(terms, bucket, bucketsPaths()[0], GapPolicy.INSERT_ZEROS);
            if (thisBucketValue != null && thisBucketValue.isInfinite() == false && thisBucketValue.isNaN() == false) {
                sum += thisBucketValue;
            }

            List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                .map((p) -> (InternalAggregation) p)
                .collect(Collectors.toList());

            aggs.add(new InternalSimpleValue(name(), sum, formatter, metadata()));

            mapAggregations.put(i, aggs);

        }

        createBucket(terms, mapAggregations, allBuckets);
    }
}
