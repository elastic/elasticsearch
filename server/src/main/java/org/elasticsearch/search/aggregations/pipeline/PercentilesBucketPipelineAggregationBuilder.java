/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PercentilesBucketPipelineAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<
    PercentilesBucketPipelineAggregationBuilder> {
    public static final String NAME = "percentiles_bucket";
    static final ParseField PERCENTS_FIELD = new ParseField("percents");
    static final ParseField KEYED_FIELD = new ParseField("keyed");

    private double[] percents = new double[] { 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0 };
    private boolean keyed = true;

    public PercentilesBucketPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public PercentilesBucketPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        percents = in.readDoubleArray();
        keyed = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(percents);
        out.writeBoolean(keyed);
    }

    /**
     * Get the percentages to calculate percentiles for in this aggregation
     */
    public double[] getPercents() {
        return percents;
    }

    /**
     * Set the percentages to calculate percentiles for in this aggregation
     */
    public PercentilesBucketPipelineAggregationBuilder setPercents(double[] percents) {
        if (percents == null) {
            throw new IllegalArgumentException("[percents] must not be null: [" + name + "]");
        }
        for (Double p : percents) {
            if (p == null || p < 0.0 || p > 100.0) {
                throw new IllegalArgumentException(
                    PERCENTS_FIELD.getPreferredName() + " must only contain non-null doubles from 0.0-100.0 inclusive"
                );
            }
        }
        this.percents = percents;
        return this;
    }

    /**
     * Set whether the XContent should be keyed
     */
    public PercentilesBucketPipelineAggregationBuilder setKeyed(boolean keyed) {
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
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new PercentilesBucketPipelineAggregator(name, percents, keyed, bucketsPaths, gapPolicy(), formatter(), metadata);
    }

    @Override
    protected void validate(ValidationContext context) {
        super.validate(context);
        for (Double p : percents) {
            if (p == null || p < 0.0 || p > 100.0) {
                context.addValidationError(
                    PERCENTS_FIELD.getPreferredName() + " must only contain non-null doubles from 0.0-100.0 inclusive"
                );
                return;
            }
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (percents != null) {
            builder.array(PERCENTS_FIELD.getPreferredName(), percents);
        }
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }

    public static final PipelineAggregator.Parser PARSER = new BucketMetricsParser() {

        @Override
        protected PercentilesBucketPipelineAggregationBuilder buildFactory(
            String pipelineAggregatorName,
            String bucketsPath,
            Map<String, Object> params
        ) {

            PercentilesBucketPipelineAggregationBuilder factory = new PercentilesBucketPipelineAggregationBuilder(
                pipelineAggregatorName,
                bucketsPath
            );

            double[] percents = (double[]) params.get(PERCENTS_FIELD.getPreferredName());
            if (percents != null) {
                factory.setPercents(percents);
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
            if (PERCENTS_FIELD.match(field, parser.getDeprecationHandler()) && token == XContentParser.Token.START_ARRAY) {
                List<Double> percents = new ArrayList<>(10);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    percents.add(parser.doubleValue());
                }
                params.put(PERCENTS_FIELD.getPreferredName(), percents.stream().mapToDouble(Double::doubleValue).toArray());
                return true;
            } else if (KEYED_FIELD.match(field, parser.getDeprecationHandler()) && token == XContentParser.Token.VALUE_BOOLEAN) {
                params.put(KEYED_FIELD.getPreferredName(), parser.booleanValue());
                return true;
            }
            return false;
        }

    };

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(percents), keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        PercentilesBucketPipelineAggregationBuilder other = (PercentilesBucketPipelineAggregationBuilder) obj;
        return Objects.deepEquals(percents, other.percents) && Objects.equals(keyed, other.keyed);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
