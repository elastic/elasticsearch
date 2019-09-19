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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class ExtendedStatsBucketPipelineAggregationBuilder
        extends BucketMetricsPipelineAggregationBuilder<ExtendedStatsBucketPipelineAggregationBuilder> {
    public static final String NAME = "extended_stats_bucket";

    private double sigma = 2.0;

    public ExtendedStatsBucketPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public ExtendedStatsBucketPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        sigma = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    /**
     * Set the value of sigma to use when calculating the standard deviation
     * bounds
     */
    public ExtendedStatsBucketPipelineAggregationBuilder sigma(double sigma) {
        if (sigma < 0.0) {
            throw new IllegalArgumentException(ExtendedStatsBucketParser.SIGMA.getPreferredName() + " must be a non-negative double");
        }
        this.sigma = sigma;
        return this;
    }

    /**
     * Get the value of sigma to use when calculating the standard deviation
     * bounds
     */
    public double sigma() {
        return sigma;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new ExtendedStatsBucketPipelineAggregator(name, bucketsPaths, sigma, gapPolicy(), formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> aggBuilders,
            Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        super.doValidate(parent, aggBuilders, pipelineAggregatorFactories);

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
    public int hashCode() {
        return Objects.hash(super.hashCode(), sigma);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExtendedStatsBucketPipelineAggregationBuilder other = (ExtendedStatsBucketPipelineAggregationBuilder) obj;
        return Objects.equals(sigma, other.sigma);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
