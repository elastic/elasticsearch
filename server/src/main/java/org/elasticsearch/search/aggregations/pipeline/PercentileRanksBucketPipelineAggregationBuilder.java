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

package org.elasticsearch.search.aggregations.pipeline;

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class PercentileRanksBucketPipelineAggregationBuilder
    extends BucketMetricsPipelineAggregationBuilder<PercentileRanksBucketPipelineAggregationBuilder> {
    public static final String NAME = "percentile_ranks_bucket";
    static final ParseField VALUES_FIELD = new ParseField("values");
    static final ParseField KEYED_FIELD = new ParseField("keyed");

    private double[] values;
    private boolean keyed = true;

    public PercentileRanksBucketPipelineAggregationBuilder(String name, String bucketsPath, double[] values) {
        super(name, NAME, new String[] { bucketsPath });
        this = this.setvalues(values);
    }

    /**
     * Read from a stream.
     */
    public PercentileRanksBucketPipelineAggregationBuilder(StreamInput in)
        throws IOException {
        super(in, NAME);
        values = in.readDoubleArray();
        keyed = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
    }

    /**
     * Get the values to calculate percentiles ranks for in this aggregation
     */
    public double[] getvalues() {
        return values;
    }

    /**
     * Set the values to calculate percentiles ranks for in this aggregation
     */
    public PercentileRanksBucketPipelineAggregationBuilder setvalues(double[] values) {
        if (values == null) {
            throw new IllegalArgumentException("[values] must not be null: [" + name + "]");
        }

        if (values.length == 0) {
            throw new IllegalArgumentException("[values] must be of length 1 or higher: [" + name + "]");
        }

        this.values = values;
        return this;
    }

    /**
     * Set whether the XContent should be keyed
     */
    public PercentileRanksBucketPipelineAggregationBuilder setKeyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /**
     * Get whether the XContent should be keyed
     */
    public boolean getKeyed() {
        return keyed;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new PercentileRanksBucketPipelineAggregator(name, values, keyed, bucketsPaths, gapPolicy(), formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, Collection<AggregationBuilder> aggFactories,
                           Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        super.doValidate(parent, aggFactories, pipelineAggregatorFactories);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (values != null) {
            builder.array(VALUES_FIELD.getPreferredName(), values);
        }
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    public static final PipelineAggregator.Parser PARSER = new BucketMetricsParser() {

        @Override
        protected PercentileRanksBucketPipelineAggregationBuilder buildFactory(String pipelineAggregatorName,
                                                                           String bucketsPath, Map<String, Object> params) {

            PercentileRanksBucketPipelineAggregationBuilder factory = new
                PercentileRanksBucketPipelineAggregationBuilder(pipelineAggregatorName, bucketsPath);

            double[] values = (double[]) params.get(VALUES_FIELD.getPreferredName());
            if (values != null) {
                factory.setvalues(values);
            }
            Boolean keyed = (Boolean) params.get(KEYED_FIELD.getPreferredName());
            if (keyed != null) {
                factory.setKeyed(keyed);
            }

            return factory;
        }

        @Override
        protected boolean token(XContentParser parser, String field, XContentParser.Token token, Map<String, Object> params)
            throws IOException {
            if (VALUES_FIELD.match(field, parser.getDeprecationHandler()) && token == XContentParser.Token.START_ARRAY) {
                DoubleArrayList values = new DoubleArrayList(10);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    values.add(parser.doubleValue());
                }
                params.put(VALUES_FIELD.getPreferredName(), values.toArray());
                return true;
            } else if (KEYED_FIELD.match(field, parser.getDeprecationHandler()) && token == XContentParser.Token.VALUE_BOOLEAN){
                params.put(KEYED_FIELD.getPreferredName(), parser.booleanValue());
                return true;
            }
            return false;
        }

    };

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(values), keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        PercentileRanksBucketPipelineAggregationBuilder other = (PercentileRanksBucketPipelineAggregationBuilder) obj;
        return Objects.deepEquals(values, other.values)
            && Objects.equals(keyed, other.keyed);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
