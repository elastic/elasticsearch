/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class MovAvgPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final int window;
    private MovAvgModel model;
    private final int predict;
    private final boolean minimize;

    MovAvgPipelineAggregator(
        String name,
        String[] bucketsPaths,
        DocValueFormat formatter,
        GapPolicy gapPolicy,
        int window,
        int predict,
        MovAvgModel model,
        boolean minimize,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.model = model;
        this.predict = predict;
        this.minimize = minimize;
    }

    /**
     * Read from a stream.
     */
    public MovAvgPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readVInt();
        predict = in.readVInt();
        model = in.readNamedWriteable(MovAvgModel.class);
        minimize = in.readBoolean();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeVInt(window);
        out.writeVInt(predict);
        out.writeNamedWriteable(model);
        out.writeBoolean(minimize);
    }

    @Override
    public String getWriteableName() {
        return MovAvgPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> histo = (InternalMultiBucketAggregation<
                ? extends InternalMultiBucketAggregation,
                ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        Number lastValidKey = 0;
        int lastValidPosition = 0;
        int counter = 0;

        // Do we need to fit the model parameters to the data?
        if (minimize) {
            assert (model.canBeMinimized());
            model = minimize(buckets, histo, model);
        }

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            // Default is to reuse existing bucket. Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            Bucket newBucket = bucket;

            if ((thisBucketValue == null || thisBucketValue.equals(Double.NaN)) == false) {

                // Some models (e.g. HoltWinters) have certain preconditions that must be met
                if (model.hasValue(values.size())) {
                    double movavg = model.next(values);

                    List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                        .map((p) -> (InternalAggregation) p)
                        .collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), movavg, formatter, metadata()));
                    newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
                }

                if (predict > 0) {
                    lastValidKey = factory.getKey(bucket);
                    lastValidPosition = counter;
                }

                values.offer(thisBucketValue);
            }
            counter += 1;
            newBuckets.add(newBucket);

        }

        if (buckets.size() > 0 && predict > 0) {
            double[] predictions = model.predict(values, predict);
            for (int i = 0; i < predictions.length; i++) {

                List<InternalAggregation> aggs;
                Number newKey = factory.nextKey(lastValidKey);

                if (lastValidPosition + i + 1 < newBuckets.size()) {
                    Bucket bucket = newBuckets.get(lastValidPosition + i + 1);

                    // Get the existing aggs in the bucket so we don't clobber data
                    aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                        .map((p) -> (InternalAggregation) p)
                        .collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, metadata()));

                    Bucket newBucket = factory.createBucket(newKey, bucket.getDocCount(), InternalAggregations.from(aggs));

                    // Overwrite the existing bucket with the new version
                    newBuckets.set(lastValidPosition + i + 1, newBucket);

                } else {
                    // Not seen before, create fresh
                    aggs = new ArrayList<>();
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, metadata()));

                    Bucket newBucket = factory.createBucket(newKey, 0, InternalAggregations.from(aggs));

                    // Since this is a new bucket, simply append it
                    newBuckets.add(newBucket);
                }
                lastValidKey = newKey;
            }
        }

        return factory.createAggregation(newBuckets);
    }

    private MovAvgModel minimize(
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets,
        MultiBucketsAggregation histo,
        MovAvgModel model
    ) {

        int counter = 0;
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        double[] test = new double[window];
        ListIterator<? extends InternalMultiBucketAggregation.InternalBucket> iter = buckets.listIterator(buckets.size());

        // We have to walk the iterator backwards because we don't know if/how many buckets are empty.
        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if ((thisBucketValue == null || thisBucketValue.equals(Double.NaN)) == false) {
                test[window - counter - 1] = thisBucketValue;
                counter += 1;
            }
        }

        // If we didn't fill the test set, we don't have enough data to minimize.
        // Just return the model with the starting coef
        if (counter < window) {
            return model;
        }

        // And do it again, for the train set. Unfortunately we have to fill an array and then
        // fill an evicting queue backwards :(

        counter = 0;
        double[] train = new double[window];

        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if ((thisBucketValue == null || thisBucketValue.equals(Double.NaN)) == false) {
                train[window - counter - 1] = thisBucketValue;
                counter += 1;
            }
        }

        // If we didn't fill the train set, we don't have enough data to minimize.
        // Just return the model with the starting coef
        if (counter < window) {
            return model;
        }

        for (double v : train) {
            values.add(v);
        }

        return SimulatedAnealingMinimizer.minimize(model, values, test);
    }
}
