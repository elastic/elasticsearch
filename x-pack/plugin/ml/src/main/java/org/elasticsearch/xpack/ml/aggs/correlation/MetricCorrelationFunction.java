/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD;

public class MetricCorrelationFunction implements CorrelationFunction {

    public static final ParseField NAME = new ParseField("metric_correlation");

    private static final ConstructingObjectParser<MetricCorrelationFunction, Void> PARSER = new ConstructingObjectParser<>(
        "count_correlation_function",
        false,
        a -> new MetricCorrelationFunction((String) a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
    }

    private final String bucketsPath;

    public MetricCorrelationFunction(String bucketsPath) {
        this.bucketsPath = bucketsPath;
    }

    public MetricCorrelationFunction(StreamInput in) throws IOException {
        this.bucketsPath = in.readString();
    }

    public static MetricCorrelationFunction fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPath);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(bucketsPath);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public int hashCode() {
        return NAME.getPreferredName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MetricCorrelationFunction other = (MetricCorrelationFunction) obj;
        return Objects.equals(bucketsPath, other.bucketsPath);
    }

    @Override
    public double execute(MlAggsHelper.DoubleBucketValues y, Aggregations aggregations) {
        MlAggsHelper.DoubleBucketValues bucketValues = MlAggsHelper.extractDoubleBucketedValues(bucketsPath, aggregations)
            .orElseThrow(() -> new AggregationExecutionException("could not find values in buckets_path [" + bucketsPath + "]"));
        if (bucketValues.getValues().length != y.getValues().length) {
            throw new AggregationExecutionException(
                "value lengths do not match; buckets_path value size ["
                    + bucketValues.getValues().length
                    + "] and number of buckets ["
                    + y.getValues().length
                    + "]. Unable to calculate correlation"
            );
        }
        if (bucketValues.getValues().length < 2) {
            throw new AggregationExecutionException(
                "to correlate, there must be at least two buckets; current bucket count [" + bucketValues.getValues().length + "]"
            );
        }
        PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
        return pearsonsCorrelation.correlation(y.getValues(), bucketValues.getValues());
    }

    @Override
    public void validate(PipelineAggregationBuilder.ValidationContext context, String aggName, String _unused) {}
}
