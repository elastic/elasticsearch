/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.movavg;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class MovAvgPipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("moving_avg");

    public final static PipelineAggregatorStreams.Stream STREAM = new PipelineAggregatorStreams.Stream() {
        @Override
        public MovAvgPipelineAggregator readResult(StreamInput in) throws IOException {
            MovAvgPipelineAggregator result = new MovAvgPipelineAggregator();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private DocValueFormat formatter;
    private GapPolicy gapPolicy;
    private int window;
    private MovAvgModel model;
    private int predict;
    private boolean minimize;

    public MovAvgPipelineAggregator() {
    }

    public MovAvgPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, GapPolicy gapPolicy,
                         int window, int predict, MovAvgModel model, boolean minimize, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.model = model;
        this.predict = predict;
        this.minimize = minimize;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalHistogram histo = (InternalHistogram) aggregation;
        List<? extends InternalHistogram.Bucket> buckets = histo.getBuckets();
        InternalHistogram.Factory<? extends InternalHistogram.Bucket> factory = histo.getFactory();

        List newBuckets = new ArrayList<>();
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        long lastValidKey = 0;
        int lastValidPosition = 0;
        int counter = 0;

        // Do we need to fit the model parameters to the data?
        if (minimize) {
            assert (model.canBeMinimized());
            model = minimize(buckets, histo, model);
        }

        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            // Default is to reuse existing bucket.  Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            InternalHistogram.Bucket newBucket = bucket;

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {

                // Some models (e.g. HoltWinters) have certain preconditions that must be met
                if (model.hasValue(values.size())) {
                    double movavg = model.next(values);

                    List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                        return (InternalAggregation) p;
                    }).collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), movavg, formatter, new ArrayList<PipelineAggregator>(), metaData()));
                    newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                            aggs), bucket.getKeyed(), bucket.getFormatter());
                }

                if (predict > 0) {
                    if (bucket.getKey() instanceof Number) {
                        lastValidKey  = ((Number) bucket.getKey()).longValue();
                    } else if (bucket.getKey() instanceof DateTime) {
                        lastValidKey = ((DateTime) bucket.getKey()).getMillis();
                    } else {
                        throw new AggregationExecutionException("Expected key of type Number or DateTime but got [" + lastValidKey + "]");
                    }
                    lastValidPosition = counter;
                }

                values.offer(thisBucketValue);
            }
            counter += 1;
            newBuckets.add(newBucket);

        }

        if (buckets.size() > 0 && predict > 0) {

            boolean keyed;
            DocValueFormat formatter;
            keyed = buckets.get(0).getKeyed();
            formatter = buckets.get(0).getFormatter();

            double[] predictions = model.predict(values, predict);
            for (int i = 0; i < predictions.length; i++) {

                List<InternalAggregation> aggs;
                long newKey = histo.getRounding().nextRoundingValue(lastValidKey);

                if (lastValidPosition + i + 1 < newBuckets.size()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) newBuckets.get(lastValidPosition + i + 1);

                    // Get the existing aggs in the bucket so we don't clobber data
                    aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                        return (InternalAggregation) p;
                    }).collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, new ArrayList<PipelineAggregator>(), metaData()));

                    InternalHistogram.Bucket newBucket = factory.createBucket(newKey, 0, new InternalAggregations(
                            aggs), keyed, formatter);

                    // Overwrite the existing bucket with the new version
                    newBuckets.set(lastValidPosition + i + 1, newBucket);

                } else {
                    // Not seen before, create fresh
                    aggs = new ArrayList<>();
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, new ArrayList<PipelineAggregator>(), metaData()));

                    InternalHistogram.Bucket newBucket = factory.createBucket(newKey, 0, new InternalAggregations(
                            aggs), keyed, formatter);

                    // Since this is a new bucket, simply append it
                    newBuckets.add(newBucket);
                }
                lastValidKey = newKey;
            }
        }

        return factory.create(newBuckets, histo);
    }

    private MovAvgModel minimize(List<? extends InternalHistogram.Bucket> buckets, InternalHistogram histo, MovAvgModel model) {

        int counter = 0;
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        double[] test = new double[window];
        ListIterator<? extends InternalHistogram.Bucket> iter = buckets.listIterator(buckets.size());

        // We have to walk the iterator backwards because we don't know if/how many buckets are empty.
        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {
                test[window - counter - 1] = thisBucketValue;
                counter += 1;
            }
        }

        // If we didn't fill the test set, we don't have enough data to minimize.
        // Just return the model with the starting coef
        if (counter < window) {
            return model;
        }

        //And do it again, for the train set.  Unfortunately we have to fill an array and then
        //fill an evicting queue backwards :(

        counter = 0;
        double[] train = new double[window];

        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {
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

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
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
}
