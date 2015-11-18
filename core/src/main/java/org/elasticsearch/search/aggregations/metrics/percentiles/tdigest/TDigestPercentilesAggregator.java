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
package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.AbstractPercentilesParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesParser;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class TDigestPercentilesAggregator extends AbstractTDigestPercentilesAggregator {

    public TDigestPercentilesAggregator(String name, Numeric valuesSource, AggregationContext context,
            Aggregator parent, double[] percents,
            double compression, boolean keyed, ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, valuesSource, context, parent, percents, compression, keyed, formatter, pipelineAggregators, metaData);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalTDigestPercentiles(name, keys, state, keyed, formatter, pipelineAggregators(), metaData());
        }
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return state.quantile(Double.parseDouble(name) / 100);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTDigestPercentiles(name, keys, new TDigestState(compression), keyed, formatter, pipelineAggregators(), metaData());
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private double[] percents = PercentilesParser.DEFAULT_PERCENTS;
        private double compression = 100.0;
        private boolean keyed = false;

        public Factory(String name) {
            super(name, InternalTDigestPercentiles.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }

        /**
         * Set the percentiles to compute.
         */
        public void percents(double[] percents) {
            double[] sortedPercents = Arrays.copyOf(percents, percents.length);
            Arrays.sort(sortedPercents);
            this.percents = sortedPercents;
        }

        /**
         * Get the percentiles to compute.
         */
        public double[] percents() {
            return percents;
        }

        /**
         * Set whether the XContent response should be keyed
         */
        public void keyed(boolean keyed) {
            this.keyed = keyed;
        }

        /**
         * Get whether the XContent response should be keyed
         */
        public boolean keyed() {
            return keyed;
        }

        /**
         * Expert: set the compression. Higher values improve accuracy but also
         * memory usage.
         */
        public void compression(double compression) {
            this.compression = compression;
        }

        /**
         * Expert: set the compression. Higher values improve accuracy but also
         * memory usage.
         */
        public double compression() {
            return compression;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new TDigestPercentilesAggregator(name, null, aggregationContext, parent, percents, compression, keyed, config.formatter(),
                    pipelineAggregators, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            return new TDigestPercentilesAggregator(name, valuesSource, aggregationContext, parent, percents, compression, keyed,
                    config.formatter(), pipelineAggregators, metaData);
        }

        @Override
        protected ValuesSourceAggregatorFactory<Numeric> innerReadFrom(String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) throws IOException {
            Factory factory = new Factory(name);
            factory.percents = in.readDoubleArray();
            factory.keyed = in.readBoolean();
            factory.compression = in.readDouble();
            return factory;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeDoubleArray(percents);
            out.writeBoolean(keyed);
            out.writeDouble(compression);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(PercentilesParser.PERCENTS_FIELD.getPreferredName(), percents);
            builder.field(AbstractPercentilesParser.KEYED_FIELD.getPreferredName(), keyed);
            builder.startObject(PercentilesMethod.TDIGEST.getName());
            builder.field(AbstractPercentilesParser.COMPRESSION_FIELD.getPreferredName(), compression);
            builder.endObject();
            return builder;
        }

        @Override
        protected boolean innerEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.deepEquals(percents, other.percents) && Objects.equals(keyed, other.keyed)
                    && Objects.equals(compression, other.compression);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(Arrays.hashCode(percents), keyed, compression);
        }
    }
}
