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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.reducers.SiblingReducer;
import org.elasticsearch.search.aggregations.reducers.derivative.DerivativeParser;
import org.elasticsearch.search.aggregations.support.AggregationContext;
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

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }

    private MaxBucketReducer() {
    }

    protected MaxBucketReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, bucketsPaths, metaData);
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    public InternalAggregation doReduce(List<Aggregation> aggregations, ReduceContext context) {
        List<String> maxBucketKeys = new ArrayList<>();
        double maxValue = Double.MIN_VALUE;
        String[] keys = maxBucketKeys.toArray(new String[maxBucketKeys.size()]);
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
                List<? extends Bucket> buckets = multiBucketsAgg.getBuckets();
                for (int i = 0; i < buckets.size(); i++) {
                    Bucket bucket = buckets.get(i);
                    Double bucketValue = resolveBucketValue(multiBucketsAgg, bucket);
                    if (bucketValue == null) {
                        throw new ElasticsearchIllegalStateException("FOUND GAP IN DATA"); // NOCOMMIT deal with gaps in data
                    } else if (bucketValue > maxValue) {
                        maxBucketKeys.clear();
                        maxBucketKeys.add(bucket.getKeyAsString());
                        maxValue = bucketValue;
                    } else if (bucketValue == maxValue) {
                        maxBucketKeys.add(bucket.getKeyAsString());
                    }
                }
            }
        }
        return new InternalBucketMetricValue(name(), keys, maxValue, formatter, Collections.EMPTY_LIST, metaData());
    }

    private Double resolveBucketValue(InternalMultiBucketAggregation multiBucketAggregation, InternalMultiBucketAggregation.Bucket bucket) {
        try {
            Object propertyValue = bucket.getProperty(multiBucketAggregation.getName(), AggregationPath.parse(bucketsPaths()[0])
                    .getPathElementsAsStringList());
            if (propertyValue instanceof Number) {
                return ((Number) propertyValue).doubleValue();
            } else if (propertyValue instanceof InternalNumericMetricsAggregation.SingleValue) {
                return ((InternalNumericMetricsAggregation.SingleValue) propertyValue).value();
            } else {
                throw new AggregationExecutionException(DerivativeParser.BUCKETS_PATH.getPreferredName()
                        + " must reference either a number value or a single value numeric metric aggregation");
            }
        } catch (InvalidAggregationPathException e) {
            return null;
        }
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;

        public Factory(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter) {
            super(name, TYPE.name(), bucketsPaths);
            this.formatter = formatter;
        }

        @Override
        protected Reducer createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                Map<String, Object> metaData) throws IOException {
            return new MaxBucketReducer(name, bucketsPaths, formatter, metaData);
        }

    }

}
