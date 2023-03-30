/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

/**
 * This pipeline aggregation gives the user the ability to script functions that "move" across a window
 * of data, instead of single data points.  It is the scripted version of MovingAvg pipeline agg.
 *
 * Through custom script contexts, we expose a number of convenience methods:
 *
 *  - max
 *  - min
 *  - sum
 *  - unweightedAvg
 *  - linearWeightedAvg
 *  - ewma
 *  - holt
 *  - holtWintersMovAvg
 *
 *  The user can also define any arbitrary logic via their own scripting, or combine with the above methods.
 */
public class MovFnPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final BucketHelpers.GapPolicy gapPolicy;
    private final Script script;
    private final String bucketsPath;
    private final int window;
    private final int shift;

    MovFnPipelineAggregator(
        String name,
        String bucketsPath,
        Script script,
        int window,
        int shift,
        DocValueFormat formatter,
        BucketHelpers.GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, new String[] { bucketsPath }, metadata);
        this.bucketsPath = bucketsPath;
        this.script = script;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.shift = shift;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> histo = (InternalMultiBucketAggregation<
                ? extends InternalMultiBucketAggregation,
                ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<MultiBucketsAggregation.Bucket> newBuckets = new ArrayList<>();

        // Initialize the script
        MovingFunctionScript.Factory scriptFactory = reduceContext.scriptService().compile(script, MovingFunctionScript.CONTEXT);
        Map<String, Object> vars = new HashMap<>();
        if (script.getParams() != null) {
            vars.putAll(script.getParams());
        }

        MovingFunctionScript executableScript = scriptFactory.newInstance();

        List<Double> values = buckets.stream()
            .map(b -> resolveBucketValue(histo, b, bucketsPaths()[0], gapPolicy))
            .filter(v -> v != null && v.isNaN() == false)
            .toList();

        int index = 0;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            // Default is to reuse existing bucket. Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            MultiBucketsAggregation.Bucket newBucket = bucket;

            if (thisBucketValue != null && thisBucketValue.isNaN() == false) {

                // The custom context mandates that the script returns a double (not Double) so we
                // don't need null checks, etc.
                int fromIndex = clamp(index - window + shift, values);
                int toIndex = clamp(index + shift, values);
                double result = executableScript.execute(
                    vars,
                    values.subList(fromIndex, toIndex).stream().mapToDouble(Double::doubleValue).toArray()
                );

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                    .map(InternalAggregation.class::cast)
                    .collect(Collectors.toCollection(ArrayList::new));
                aggs.add(new InternalSimpleValue(name(), result, formatter, metadata()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
                index++;
            }
            newBuckets.add(newBucket);
        }

        return factory.createAggregation(newBuckets);
    }

    private static int clamp(int index, List<Double> list) {
        if (index < 0) {
            return 0;
        }
        if (index > list.size()) {
            return list.size();
        }
        return index;
    }
}
