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

package org.elasticsearch.search.aggregations.pipeline.derivative;

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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class DerivativePipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("derivative");

    public final static PipelineAggregatorStreams.Stream STREAM = new PipelineAggregatorStreams.Stream() {
        @Override
        public DerivativePipelineAggregator readResult(StreamInput in) throws IOException {
            DerivativePipelineAggregator result = new DerivativePipelineAggregator();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private DocValueFormat formatter;
    private GapPolicy gapPolicy;
    private Double xAxisUnits;

    public DerivativePipelineAggregator() {
    }

    public DerivativePipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, GapPolicy gapPolicy, Long xAxisUnits,
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
            if (lastBucketValue != null && thisBucketValue != null) {
                double gradient = thisBucketValue - lastBucketValue;
                double xDiff = -1;
                if (xAxisUnits != null) {
                    xDiff = (thisBucketKey - lastBucketKey) / xAxisUnits;
                }
                final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                aggs.add(new InternalDerivative(name(), gradient, xDiff, formatter, new ArrayList<PipelineAggregator>(), metaData()));
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
            throw new AggregationExecutionException("InternalBucket keys must be either a Number or a DateTime for aggregation " + name()
                    + ". Found bucket with key " + key);
        }
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        if (in.readBoolean()) {
            xAxisUnits = in.readDouble();
        } else {
            xAxisUnits = null;

        }
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        boolean hasXAxisUnitsValue = xAxisUnits != null;
        out.writeBoolean(hasXAxisUnitsValue);
        if (hasXAxisUnitsValue) {
            out.writeDouble(xAxisUnits);
        }
    }
}
