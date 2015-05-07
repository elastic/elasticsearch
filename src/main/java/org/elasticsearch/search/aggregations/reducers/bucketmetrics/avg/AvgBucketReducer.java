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

package org.elasticsearch.search.aggregations.reducers.bucketmetrics.avg;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.reducers.InternalSimpleValue;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.reducers.bucketmetrics.BucketMetricsReducer;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AvgBucketReducer extends BucketMetricsReducer {

    public final static Type TYPE = new Type("avg_bucket");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public AvgBucketReducer readResult(StreamInput in) throws IOException {
            AvgBucketReducer result = new AvgBucketReducer();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }

    private int count = 0;
    private double sum = 0;

    private AvgBucketReducer() {
    }

    protected AvgBucketReducer(String name, String[] bucketsPaths, GapPolicy gapPolicy, @Nullable ValueFormatter formatter,
            Map<String, Object> metaData) {
        super(name, bucketsPaths, gapPolicy, formatter, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void preCollection() {
        count = 0;
        sum = 0;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        count++;
        sum += bucketValue;
    }

    @Override
    protected InternalAggregation buildAggregation(List<Reducer> reducers, Map<String, Object> metadata) {
        double avgValue = count == 0 ? Double.NaN : (sum / count);
        return new InternalSimpleValue(name(), avgValue, formatter, reducers, metadata);
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
            return new AvgBucketReducer(name, bucketsPaths, gapPolicy, formatter, metaData);
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
