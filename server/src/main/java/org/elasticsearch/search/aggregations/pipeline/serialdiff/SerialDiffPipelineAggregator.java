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

package org.elasticsearch.search.aggregations.pipeline.serialdiff;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class SerialDiffPipelineAggregator extends PipelineAggregator {
    private DocValueFormat formatter;
    private GapPolicy gapPolicy;
    private int lag;

    public SerialDiffPipelineAggregator(String name, String[] bucketsPaths, @Nullable DocValueFormat formatter, GapPolicy gapPolicy,
                                        int lag, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.lag = lag;
    }

    /**
     * Read from a stream.
     */
    public SerialDiffPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        lag = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeVInt(lag);
    }

    @Override
    public String getWriteableName() {
        return SerialDiffPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
                histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
                InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        EvictingQueue<Double> lagWindow = new EvictingQueue<>(lag);
        int counter = 0;

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            Bucket newBucket = bucket;

            counter += 1;

            // Still under the initial lag period, add nothing and move on
            Double lagValue;
            if (counter <= lag) {
                lagValue = Double.NaN;
            } else {
                lagValue = lagWindow.peek();  // Peek here, because we rely on add'ing to always move the window
            }

            // Normalize null's to NaN
            if (thisBucketValue == null) {
                thisBucketValue = Double.NaN;
            }

            // Both have values, calculate diff and replace the "empty" bucket
            if (!Double.isNaN(thisBucketValue) && !Double.isNaN(lagValue)) {
                double diff = thisBucketValue - lagValue;

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map(
                        (p) -> (InternalAggregation) p).collect(Collectors.toList());
                aggs.add(new InternalSimpleValue(name(), diff, formatter, new ArrayList<>(), metaData()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), new InternalAggregations(aggs));
            }

            newBuckets.add(newBucket);
            lagWindow.add(thisBucketValue);
        }
        return factory.createAggregation(newBuckets);
    }
}
