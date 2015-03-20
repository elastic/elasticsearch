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

package org.elasticsearch.search.aggregations.reducers.diff;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.reducers.InternalSimpleValue;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.resolveBucketValue;

public class DiffReducer extends Reducer {

    public final static Type TYPE = new Type("diff");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public DiffReducer readResult(StreamInput in) throws IOException {
            DiffReducer result = new DiffReducer();
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
    private int periods;

    public DiffReducer() {
    }

    public DiffReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                       int periods, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.periods = periods;
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
        int current = 0;
        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            if (current < periods) {
                newBuckets.add(bucket);
            } else {

                Double lastPeriodValue = resolveBucketValue(histo, buckets.get(current - periods), bucketsPaths()[0], gapPolicy);
                if (lastPeriodValue == null) {
                    newBuckets.add(bucket);
                } else {
                    double diff = thisBucketValue - lastPeriodValue;

                    List<InternalAggregation> aggs = new ArrayList<>(Lists.transform(bucket.getAggregations().asList(), FUNCTION));
                    aggs.add(new InternalSimpleValue(name(), diff, formatter, new ArrayList<Reducer>(), metaData()));
                    InternalHistogram.Bucket newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                            aggs), bucket.getKeyed(), bucket.getFormatter());
                    newBuckets.add(newBucket);
                }
            }
            current += 1;
        }
        return factory.create(newBuckets, histo);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        periods = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        out.writeVInt(periods);
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;
        private GapPolicy gapPolicy;
        private int periods;

        public Factory(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy, int periods) {
            super(name, TYPE.name(), bucketsPaths);
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
            this.periods = periods;
        }

        @Override
        protected Reducer createInternal(Map<String, Object> metaData) throws IOException {
            return new DiffReducer(name, bucketsPaths, formatter, gapPolicy, periods, metaData);
        }

    }
}
