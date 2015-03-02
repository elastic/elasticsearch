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

package org.elasticsearch.search.aggregations.reducers.movavg;

import com.google.common.base.Function;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.reducers.*;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.resolveBucketValue;

public class MovAvgReducer extends Reducer {

    public final static Type TYPE = new Type("movavg");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public MovAvgReducer readResult(StreamInput in) throws IOException {
            MovAvgReducer result = new MovAvgReducer();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }

    private static final Function<Aggregation, InternalAggregation> FUNCTION = new Function<Aggregation, InternalAggregation>() {
        @Override
        public InternalAggregation apply(Aggregation input) {
            return (InternalAggregation) input;
        }
    };

    private ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private MovAvgModel.Weighting weightingType;
    private int window;

    public MovAvgReducer() {
    }

    public MovAvgReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                         MovAvgModel.Weighting weightingType, int window, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.weightingType = weightingType;
        this.window = window;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalHistogram<? extends InternalHistogram.Bucket> histo = (InternalHistogram<? extends InternalHistogram.Bucket>) aggregation;
        List<? extends InternalHistogram.Bucket> buckets = histo.getBuckets();
        InternalHistogram.Factory<? extends InternalHistogram.Bucket> factory = histo.getFactory();

        List newBuckets = new ArrayList<>();
        EvictingQueue<Double> values = EvictingQueue.create(this.window);

        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            values.offer(thisBucketValue);

            // TODO handle "edge policy"
            double movavg = MovAvgModel.next(values, this.weightingType);

            List<InternalAggregation> aggs = new ArrayList<>(Lists.transform(bucket.getAggregations().asList(), FUNCTION));
            aggs.add(new InternalSimpleValue(name(), movavg, formatter, new ArrayList<Reducer>(), metaData()));
            InternalHistogram.Bucket newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                    aggs), bucket.getKeyed(), bucket.getFormatter());
            newBuckets.add(newBucket);

        }
        return factory.create(histo.getName(), newBuckets, histo);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        weightingType = MovAvgModel.Weighting.readFrom(in);
        window = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        weightingType.writeTo(out);
        out.writeVInt(window);
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;
        private GapPolicy gapPolicy;
        private MovAvgModel.Weighting weightingType;
        private int window;

        public Factory(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                       MovAvgModel.Weighting weightingType, int window) {
            super(name, TYPE.name(), bucketsPaths);
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
            this.weightingType = weightingType;
            this.window = window;
        }

        @Override
        protected Reducer createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                Map<String, Object> metaData) throws IOException {
            return new MovAvgReducer(name, bucketsPaths, formatter, gapPolicy, weightingType, window, metaData);
        }

    }
}
