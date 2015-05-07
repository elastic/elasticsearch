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

package org.elasticsearch.search.aggregations.reducers.bucketmetrics.max;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.reducers.bucketmetrics.BucketMetricsReducer;
import org.elasticsearch.search.aggregations.reducers.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MaxBucketReducer extends BucketMetricsReducer {

    public final static Type TYPE = new Type("max_bucket");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public MaxBucketReducer readResult(StreamInput in) throws IOException {
            MaxBucketReducer result = new MaxBucketReducer();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }

    private List<String> maxBucketKeys;
    private double maxValue;

    private MaxBucketReducer() {
    }

    protected MaxBucketReducer(String name, String[] bucketsPaths, GapPolicy gapPolicy, @Nullable ValueFormatter formatter,
            Map<String, Object> metaData) {
        super(name, bucketsPaths, gapPolicy, formatter, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void preCollection() {
        maxBucketKeys = new ArrayList<>();
        maxValue = Double.NEGATIVE_INFINITY;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        if (bucketValue > maxValue) {
            maxBucketKeys.clear();
            maxBucketKeys.add(bucketKey);
            maxValue = bucketValue;
        } else if (bucketValue.equals(maxValue)) {
            maxBucketKeys.add(bucketKey);
        }
    }

    @Override
    protected InternalAggregation buildAggregation(List<Reducer> reducers, Map<String, Object> metadata) {
        String[] keys = maxBucketKeys.toArray(new String[maxBucketKeys.size()]);
        return new InternalBucketMetricValue(name(), keys, maxValue, formatter, Collections.EMPTY_LIST, metaData());
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
