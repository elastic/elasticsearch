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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile;

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsPipelineAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PercentilesBucketPipelineAggregationBuilder
        extends BucketMetricsPipelineAggregationBuilder<PercentilesBucketPipelineAggregationBuilder> {
    public static final String NAME = "percentiles_bucket";
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private static final ParseField PERCENTS_FIELD = new ParseField("percents");

    private double[] percents = new double[] { 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0 };

    public PercentilesBucketPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public PercentilesBucketPipelineAggregationBuilder(StreamInput in)
            throws IOException {
        super(in, NAME);
        percents = in.readDoubleArray();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(percents);
    }

    /**
     * Get the percentages to calculate percentiles for in this aggregation
     */
    public double[] percents() {
        return percents;
    }

    /**
     * Set the percentages to calculate percentiles for in this aggregation
     */
    public PercentilesBucketPipelineAggregationBuilder percents(double[] percents) {
        if (percents == null) {
            throw new IllegalArgumentException("[percents] must not be null: [" + name + "]");
        }
        for (Double p : percents) {
            if (p == null || p < 0.0 || p > 100.0) {
                throw new IllegalArgumentException(PERCENTS_FIELD.getPreferredName()
                        + " must only contain non-null doubles from 0.0-100.0 inclusive");
            }
        }
        this.percents = percents;
        return this;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new PercentilesBucketPipelineAggregator(name, percents, bucketsPaths, gapPolicy(), formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }

        for (Double p : percents) {
            if (p == null || p < 0.0 || p > 100.0) {
                throw new IllegalStateException(PERCENTS_FIELD.getPreferredName()
                        + " must only contain non-null doubles from 0.0-100.0 inclusive");
            }
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (percents != null) {
            builder.field(PERCENTS_FIELD.getPreferredName(), percents);
        }
        return builder;
    }

    public static final PipelineAggregator.Parser PARSER = new BucketMetricsParser() {

        @Override
        protected PercentilesBucketPipelineAggregationBuilder buildFactory(String pipelineAggregatorName,
                                                                          String bucketsPath, Map<String, Object> params) {

            PercentilesBucketPipelineAggregationBuilder factory = new
                PercentilesBucketPipelineAggregationBuilder(pipelineAggregatorName, bucketsPath);

            double[] percents = (double[]) params.get(PERCENTS_FIELD.getPreferredName());
            if (percents != null) {
                factory.percents(percents);
            }

            return factory;
        }

        @Override
        protected boolean token(XContentParser parser, QueryParseContext context, String field,
                                XContentParser.Token token, Map<String, Object> params) throws IOException {
            if (context.getParseFieldMatcher().match(field, PERCENTS_FIELD) && token == XContentParser.Token.START_ARRAY) {
                DoubleArrayList percents = new DoubleArrayList(10);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    percents.add(parser.doubleValue());
                }
                params.put(PERCENTS_FIELD.getPreferredName(), percents.toArray());
                return true;
            }
            return false;
        }

    };

    @Override
    protected int innerHashCode() {
        return Arrays.hashCode(percents);
    }

    @Override
    protected boolean innerEquals(BucketMetricsPipelineAggregationBuilder<PercentilesBucketPipelineAggregationBuilder> obj) {
        PercentilesBucketPipelineAggregationBuilder other = (PercentilesBucketPipelineAggregationBuilder) obj;
        return Objects.deepEquals(percents, other.percents);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
