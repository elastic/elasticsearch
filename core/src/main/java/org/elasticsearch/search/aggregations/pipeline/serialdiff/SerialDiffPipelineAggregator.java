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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class SerialDiffPipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("serial_diff");

    public final static PipelineAggregatorStreams.Stream STREAM = in -> {
        SerialDiffPipelineAggregator result = new SerialDiffPipelineAggregator();
        result.readFrom(in);
        return result;
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private int lag;

    public SerialDiffPipelineAggregator() {
    }

    public SerialDiffPipelineAggregator(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                                        int lag, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.lag = lag;
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
        EvictingQueue<Double> lagWindow = new EvictingQueue<>(lag);
        int counter = 0;

        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            InternalHistogram.Bucket newBucket = bucket;

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

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                aggs.add(new InternalSimpleValue(name(), diff, formatter, new ArrayList<PipelineAggregator>(), metaData()));
                newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                        aggs), bucket.getKeyed(), bucket.getFormatter());
            }


            newBuckets.add(newBucket);
            lagWindow.add(thisBucketValue);

        }
        return factory.create(newBuckets, histo);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        lag = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        out.writeVInt(lag);
    }

    public static class Factory extends PipelineAggregatorFactory {

        private String format;
        private GapPolicy gapPolicy = GapPolicy.SKIP;
        private int lag = 1;

        public Factory(String name, String[] bucketsPaths) {
            super(name, TYPE.name(), bucketsPaths);
        }

        /**
         * Sets the lag to use when calculating the serial difference.
         */
        public void lag(int lag) {
            this.lag = lag;
        }

        /**
         * Gets the lag to use when calculating the serial difference.
         */
        public int lag() {
            return lag;
        }

        /**
         * Sets the format to use on the output of this aggregation.
         */
        public void format(String format) {
            this.format = format;
        }

        /**
         * Gets the format to use on the output of this aggregation.
         */
        public String format() {
            return format;
        }

        /**
         * Sets the GapPolicy to use on the output of this aggregation.
         */
        public void gapPolicy(GapPolicy gapPolicy) {
            this.gapPolicy = gapPolicy;
        }

        /**
         * Gets the GapPolicy to use on the output of this aggregation.
         */
        public GapPolicy gapPolicy() {
            return gapPolicy;
        }

        protected ValueFormatter formatter() {
            if (format != null) {
                return ValueFormat.Patternable.Number.format(format).formatter();
            } else {
                return ValueFormatter.RAW;
            }
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
            return new SerialDiffPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, lag, metaData);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            if (format != null) {
                builder.field(SerialDiffParser.FORMAT.getPreferredName(), format);
            }
            builder.field(SerialDiffParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
            builder.field(SerialDiffParser.LAG.getPreferredName(), lag);
            return builder;
        }

        @Override
        protected PipelineAggregatorFactory doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
            Factory factory = new Factory(name, bucketsPaths);
            factory.format = in.readOptionalString();
            factory.gapPolicy = GapPolicy.readFrom(in);
            factory.lag = in.readVInt();
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeOptionalString(format);
            gapPolicy.writeTo(out);
            out.writeVInt(lag);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(format, gapPolicy, lag);
        }
        @Override
        protected boolean doEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(format, other.format)
                    && Objects.equals(gapPolicy, other.gapPolicy)
                    && Objects.equals(lag, other.lag);
        }

    }
}
