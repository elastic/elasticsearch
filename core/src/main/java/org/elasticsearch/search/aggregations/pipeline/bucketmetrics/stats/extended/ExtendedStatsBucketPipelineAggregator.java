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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsFactory;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsPipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExtendedStatsBucketPipelineAggregator extends BucketMetricsPipelineAggregator {

    public final static Type TYPE = new Type("extended_stats_bucket");

    public final static PipelineAggregatorStreams.Stream STREAM = new PipelineAggregatorStreams.Stream() {
        @Override
        public ExtendedStatsBucketPipelineAggregator readResult(StreamInput in) throws IOException {
            ExtendedStatsBucketPipelineAggregator result = new ExtendedStatsBucketPipelineAggregator();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
        InternalExtendedStatsBucket.registerStreams();
    }

    private double sum = 0;
    private long count = 0;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double sumOfSqrs = 1;
    private double sigma;

    protected ExtendedStatsBucketPipelineAggregator(String name, String[] bucketsPaths, double sigma, GapPolicy gapPolicy,
                                                    ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, bucketsPaths, gapPolicy, formatter, metaData);
        this.sigma = sigma;
    }

    ExtendedStatsBucketPipelineAggregator() {
        // For Serialization
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void preCollection() {
        sum = 0;
        count = 0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        sumOfSqrs = 1;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        sum += bucketValue;
        min = Math.min(min, bucketValue);
        max = Math.max(max, bucketValue);
        count += 1;
        sumOfSqrs += bucketValue * bucketValue;
    }

    @Override
    protected InternalAggregation buildAggregation(List<PipelineAggregator> pipelineAggregators, Map<String, Object> metadata) {
        return new InternalExtendedStatsBucket(name(), count, sum, min, max, sumOfSqrs, sigma, formatter, pipelineAggregators, metadata);
    }

    public static class Factory extends BucketMetricsFactory {

        private double sigma = 2.0;

        public Factory(String name, String[] bucketsPaths) {
            super(name, TYPE.name(), bucketsPaths);
        }

        /**
         * Set the value of sigma to use when calculating the standard deviation
         * bounds
         */
        public void sigma(double sigma) {
            this.sigma = sigma;
        }

        /**
         * Get the value of sigma to use when calculating the standard deviation
         * bounds
         */
        public double sigma() {
            return sigma;
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
            return new ExtendedStatsBucketPipelineAggregator(name, bucketsPaths, sigma, gapPolicy(), formatter(), metaData);
        }

        @Override
        public void doValidate(AggregatorFactory parent, AggregatorFactory[] aggFactories,
                List<PipelineAggregatorFactory> pipelineAggregatorFactories) {
            if (bucketsPaths.length != 1) {
                throw new IllegalStateException(Parser.BUCKETS_PATH.getPreferredName()
                        + " must contain a single entry for aggregation [" + name + "]");
            }

            if (sigma < 0.0 ) {
                throw new IllegalStateException(ExtendedStatsBucketParser.SIGMA.getPreferredName()
                        + " must be a non-negative double");
            }
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(ExtendedStatsBucketParser.SIGMA.getPreferredName(), sigma);
            return builder;
        }

        @Override
        protected BucketMetricsFactory innerReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
            Factory factory = new Factory(name, bucketsPaths);
            factory.sigma = in.readDouble();
            return factory;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeDouble(sigma);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(sigma);
        }

        @Override
        protected boolean innerEquals(BucketMetricsFactory obj) {
            Factory other = (Factory) obj;
            return Objects.equals(sigma, other.sigma);
        }

    }

}
