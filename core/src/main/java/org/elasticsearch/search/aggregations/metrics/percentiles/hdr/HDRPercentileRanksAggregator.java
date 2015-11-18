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
package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.AbstractPercentilesParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;
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
public class HDRPercentileRanksAggregator extends AbstractHDRPercentilesAggregator {

    public HDRPercentileRanksAggregator(String name, Numeric valuesSource, AggregationContext context, Aggregator parent,
            double[] percents, int numberOfSignificantValueDigits, boolean keyed, ValueFormatter formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, valuesSource, context, parent, percents, numberOfSignificantValueDigits, keyed, formatter, pipelineAggregators,
                metaData);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        DoubleHistogram state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalHDRPercentileRanks(name, keys, state, keyed, formatter, pipelineAggregators(), metaData());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        DoubleHistogram state;
        state = new DoubleHistogram(numberOfSignificantValueDigits);
        state.setAutoResize(true);
        return new InternalHDRPercentileRanks(name, keys, state,
                keyed, formatter, pipelineAggregators(), metaData());
    }

    @Override
    public double metric(String name, long bucketOrd) {
        DoubleHistogram state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return InternalHDRPercentileRanks.percentileRank(state, Double.valueOf(name));
        }
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private double[] values;
        private int numberOfSignificantValueDigits = 3;
        private boolean keyed = false;

        public Factory(String name) {
            super(name, InternalHDRPercentileRanks.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }

        /**
         * Set the values to compute percentiles from.
         */
        public void values(double[] values) {
            double[] sortedValues = Arrays.copyOf(values, values.length);
            Arrays.sort(sortedValues);
            this.values = sortedValues;
        }

        /**
         * Get the values to compute percentiles from.
         */
        public double[] values() {
            return values;
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
         * Expert: set the number of significant digits in the values.
         */
        public void numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
            this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        }

        /**
         * Expert: set the number of significant digits in the values.
         */
        public int numberOfSignificantValueDigits() {
            return numberOfSignificantValueDigits;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new HDRPercentileRanksAggregator(name, null, aggregationContext, parent, values, numberOfSignificantValueDigits, keyed,
                    config.formatter(), pipelineAggregators, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            return new HDRPercentileRanksAggregator(name, valuesSource, aggregationContext, parent, values, numberOfSignificantValueDigits,
                    keyed, config.formatter(), pipelineAggregators, metaData);
        }

        @Override
        protected ValuesSourceAggregatorFactory<Numeric> innerReadFrom(String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) throws IOException {
            Factory factory = new Factory(name);
            factory.values = in.readDoubleArray();
            factory.keyed = in.readBoolean();
            factory.numberOfSignificantValueDigits = in.readVInt();
            return factory;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeDoubleArray(values);
            out.writeBoolean(keyed);
            out.writeVInt(numberOfSignificantValueDigits);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(PercentileRanksParser.VALUES_FIELD.getPreferredName(), values);
            builder.field(AbstractPercentilesParser.KEYED_FIELD.getPreferredName(), keyed);
            builder.startObject(PercentilesMethod.HDR.getName());
            builder.field(AbstractPercentilesParser.NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
            builder.endObject();
            return builder;
        }

        @Override
        protected boolean innerEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.deepEquals(values, other.values) && Objects.equals(keyed, other.keyed)
                    && Objects.equals(numberOfSignificantValueDigits, other.numberOfSignificantValueDigits);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(Arrays.hashCode(values), keyed, numberOfSignificantValueDigits);
        }
    }
}
