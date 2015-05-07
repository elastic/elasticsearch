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

import com.google.common.collect.Lists;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.reducers.ReducerStreams;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.resolveBucketValue;

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

    private ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private Double xAxisUnits;

    public DerivativeReducer() {
    }

    public DerivativeReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy, Long xAxisUnits,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.xAxisUnits = xAxisUnits == null ? null : (double) xAxisUnits;
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
        Long lastBucketKey = null;
        Double lastBucketValue = null;
        for (InternalHistogram.Bucket bucket : buckets) {
            Long thisBucketKey = resolveBucketKeyAsLong(bucket);
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            if (lastBucketValue != null) {
                double gradient = thisBucketValue - lastBucketValue;
                double xDiff = -1;
                if (xAxisUnits != null) {
                    xDiff = (thisBucketKey - lastBucketKey) / xAxisUnits;
                }
                List<InternalAggregation> aggs = new ArrayList<>(Lists.transform(bucket.getAggregations().asList(),
                        AGGREGATION_TRANFORM_FUNCTION));
                aggs.add(new InternalDerivative(name(), gradient, xDiff, formatter, new ArrayList<Reducer>(), metaData()));
                InternalHistogram.Bucket newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                        aggs), bucket.getKeyed(), bucket.getFormatter());
                newBuckets.add(newBucket);
            } else {
                newBuckets.add(bucket);
            }
            lastBucketKey = thisBucketKey;
            lastBucketValue = thisBucketValue;
        }
        return factory.create(newBuckets, histo);
    }

    private Long resolveBucketKeyAsLong(InternalHistogram.Bucket bucket) {
        Object key = bucket.getKey();
        if (key instanceof DateTime) {
            return ((DateTime) key).getMillis();
        } else if (key instanceof Number) {
            return ((Number) key).longValue();
        } else {
            throw new AggregationExecutionException("Bucket keys must be either a Number or a DateTime for aggregation " + name()
                    + ". Found bucket with key " + key);
        }
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        if (in.readBoolean()) {
            xAxisUnits = in.readDouble();
        } else {
            xAxisUnits = null;

        }
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        boolean hasXAxisUnitsValue = xAxisUnits != null;
        out.writeBoolean(hasXAxisUnitsValue);
        if (hasXAxisUnitsValue) {
            out.writeDouble(xAxisUnits);
        }
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;
        private GapPolicy gapPolicy;
        private Long xAxisUnits;

        public Factory(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy, Long xAxisUnits) {
            super(name, TYPE.name(), bucketsPaths);
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
            this.xAxisUnits = xAxisUnits;
        }

        @Override
        protected Reducer createInternal(Map<String, Object> metaData) throws IOException {
            return new DerivativeReducer(name, bucketsPaths, formatter, gapPolicy, xAxisUnits, metaData);
        }

        @Override
        public void doValidate(AggregatorFactory parent, AggregatorFactory[] aggFactories, List<ReducerFactory> reducerFactories) {
            if (bucketsPaths.length != 1) {
                throw new IllegalStateException(Reducer.Parser.BUCKETS_PATH.getPreferredName()
                        + " must contain a single entry for reducer [" + name + "]");
            }
            if (!(parent instanceof HistogramAggregator.Factory)) {
                throw new IllegalStateException("derivative reducer [" + name
                        + "] must have a histogram or date_histogram as parent");
            } else {
                HistogramAggregator.Factory histoParent = (HistogramAggregator.Factory) parent;
                if (histoParent.minDocCount() != 0) {
                    throw new IllegalStateException("parent histogram of derivative reducer [" + name
                            + "] must have min_doc_count of 0");
                }
            }
        }

    }
}
