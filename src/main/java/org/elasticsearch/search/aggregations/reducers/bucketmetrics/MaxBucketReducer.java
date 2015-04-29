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

package org.elasticsearch.search.aggregations.reducers.bucketmetrics;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.reducers.SiblingReducer;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MaxBucketReducer extends SiblingReducer {

    public final static Type TYPE = new Type("max_bucket");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public MaxBucketReducer readResult(StreamInput in) throws IOException {
            MaxBucketReducer result = new MaxBucketReducer();
            result.readFrom(in);
            return result;
        }
    };

    private ValueFormatter formatter;
    private GapPolicy gapPolicy;

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }

    private MaxBucketReducer() {
    }

    protected MaxBucketReducer(String name, String[] bucketsPaths, GapPolicy gapPolicy, @Nullable ValueFormatter formatter,
            Map<String, Object> metaData) {
        super(name, bucketsPaths, metaData);
        this.gapPolicy = gapPolicy;
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    public InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        List<String> maxBucketKeys = new ArrayList<>();
        double maxValue = Double.NEGATIVE_INFINITY;
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
                List<? extends Bucket> buckets = multiBucketsAgg.getBuckets();
                for (int i = 0; i < buckets.size(); i++) {
                    Bucket bucket = buckets.get(i);
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, bucketsPath, gapPolicy);
                    if (bucketValue != null) {
                        if (bucketValue > maxValue) {
                            maxBucketKeys.clear();
                            maxBucketKeys.add(bucket.getKeyAsString());
                            maxValue = bucketValue;
                        } else if (bucketValue.equals(maxValue)) {
                            maxBucketKeys.add(bucket.getKeyAsString());
                        }
                    }
                }
            }
        }
        String[] keys = maxBucketKeys.toArray(new String[maxBucketKeys.size()]);
        return new InternalBucketMetricValue(name(), keys, maxValue, formatter, Collections.EMPTY_LIST, metaData());
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;
        private final GapPolicy gapPolicy;

        public Factory(String name, String[] bucketsPaths, GapPolicy gapPolicy, @Nullable ValueFormatter formatter) {
            super(name, TYPE.name(), bucketsPaths);
            this.gapPolicy = gapPolicy;
            this.formatter = formatter;
        }

        @Override
        protected Reducer createInternal(Map<String, Object> metaData) throws IOException {
            return new MaxBucketReducer(name, bucketsPaths, gapPolicy, formatter, metaData);
        }

        @Override
        public void doValidate(AggregatorFactory parent, AggregatorFactory[] aggFactories, List<ReducerFactory> reducerFactories) {
            if (bucketsPaths.length != 1) {
                throw new IllegalStateException(Reducer.Parser.BUCKETS_PATH.getPreferredName()
                        + " must contain a single entry for reducer [" + name + "]");
            }
        }

    }

}
