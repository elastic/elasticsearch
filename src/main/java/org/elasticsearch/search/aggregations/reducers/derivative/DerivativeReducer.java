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

package org.elasticsearch.search.aggregations.reducers.derivative;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

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
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.reducers.InternalSimpleValue;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DerivativeReducer extends Reducer {

    public final static Type TYPE = new Type("derivative");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public DerivativeReducer readResult(StreamInput in) throws IOException {
            DerivativeReducer result = new DerivativeReducer();
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

    public DerivativeReducer() {
    }

    public DerivativeReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
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
        Double lastBucketValue = null;
        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket);
            if (lastBucketValue != null) {
                if (thisBucketValue == null) {
                    throw new ElasticsearchIllegalStateException("FOUND GAP IN DATA"); // NOCOMMIT deal with gaps in data
                }
                double diff = thisBucketValue - lastBucketValue;

                List<InternalAggregation> aggs = new ArrayList<>(Lists.transform(bucket.getAggregations().asList(), FUNCTION));
                aggs.add(new InternalSimpleValue(name(), diff, formatter, new ArrayList<Reducer>(), metaData()));
                InternalHistogram.Bucket newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                        aggs), bucket.getKeyed(), bucket.getFormatter());
                newBuckets.add(newBucket);
            } else {
                newBuckets.add(bucket);
            }
            lastBucketValue = thisBucketValue;
        }
        return factory.create(histo.getName(), newBuckets, histo);
    }

    private Double resolveBucketValue(InternalHistogram<? extends InternalHistogram.Bucket> histo, InternalHistogram.Bucket bucket) {
        try {
            Object propertyValue = bucket.getProperty(histo.getName(), AggregationPath.parse(bucketsPaths()[0])
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
            return new DerivativeReducer(name, bucketsPaths, formatter, metaData);
        }

    }
}
