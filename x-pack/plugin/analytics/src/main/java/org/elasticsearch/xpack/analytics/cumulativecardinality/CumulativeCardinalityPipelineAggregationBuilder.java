/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsParser;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

public class CumulativeCardinalityPipelineAggregationBuilder
        extends AbstractPipelineAggregationBuilder<CumulativeCardinalityPipelineAggregationBuilder> {
    public static final String NAME = "cumulative_cardinality";

    public static final ConstructingObjectParser<CumulativeCardinalityPipelineAggregationBuilder, String> PARSER =
            new ConstructingObjectParser<>(NAME, false, (args, name) -> {
                if (AnalyticsPlugin.getLicenseState().isDataScienceAllowed() == false) {
                    throw LicenseUtils.newComplianceException(XPackField.ANALYTICS);
                }

                // Increment usage here since it is a good boundary between internal and external, and should correlate 1:1 with
                // usage and not internal instantiations
                AnalyticsPlugin.cumulativeCardUsage.incrementAndGet();

                return new CumulativeCardinalityPipelineAggregationBuilder(name, (String) args[0]);
            });
    static {
        PARSER.declareString(constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareString(CumulativeCardinalityPipelineAggregationBuilder::format, FORMAT);
    }

    private String format;

    public CumulativeCardinalityPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public CumulativeCardinalityPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        format = in.readOptionalString();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public CumulativeCardinalityPipelineAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new CumulativeCardinalityPipelineAggregator(name, bucketsPaths, formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> aggFactories,
                           Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(BUCKETS_PATH.getPreferredName()
                + " must contain a single entry for aggregation [" + name + "]");
        }

        validateSequentiallyOrderedParentAggs(parent, NAME, name);
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CumulativeCardinalityPipelineAggregationBuilder other = (CumulativeCardinalityPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
