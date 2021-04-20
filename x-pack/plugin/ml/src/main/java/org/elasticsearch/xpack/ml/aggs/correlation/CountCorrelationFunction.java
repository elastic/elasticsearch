/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Objects;

public class CountCorrelationFunction implements CorrelationFunction {

    public static final ParseField NAME = new ParseField("count_correlation");
    public static final ParseField INDICATOR = new ParseField("indicator");

    private static final ConstructingObjectParser<CountCorrelationFunction, Void> PARSER = new ConstructingObjectParser<>(
        "count_correlation_function",
        false,
        a -> new CountCorrelationFunction((CorrelativeValue)a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> CorrelativeValue.fromXContent(p), INDICATOR);
    }

    private final CorrelativeValue indicator;

    public CountCorrelationFunction(CorrelativeValue indicator) {
        this.indicator = indicator;
    }

    public CountCorrelationFunction(StreamInput in) throws IOException {
        this.indicator = new CorrelativeValue(in);
    }

    public static CountCorrelationFunction fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDICATOR.getPreferredName(), indicator);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indicator.writeTo(out);
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
        CountCorrelationFunction other = (CountCorrelationFunction) obj;
        return Objects.equals(indicator, other.indicator);
    }

    @Override
    public double execute(CorrelativeValue y) {
        if (indicator.getExpectations().length != y.getExpectations().length) {
            throw new AggregationExecutionException(
                "value lengths do not match ["
                    + indicator.getExpectations().length
                    + "] and ["
                    + y.getExpectations().length
                    + "] unable to calculate correlation"
            );
        }
        final double xMean;
        final double xVar;
        if (indicator.getFractions() == null) {
            double sum = CorrelationFunction.sum(indicator.getExpectations());
            xMean = sum / indicator.getExpectations().length;
            double var = 0;
            for (double v : indicator.getExpectations()) {
                var += Math.pow(v - xMean, 2);
            }
            xVar = var / indicator.getExpectations().length;
        } else {
            double mean = 0;
            for (int i = 0; i < indicator.getExpectations().length; i++) {
                mean += indicator.getExpectations()[i] * indicator.getFractions()[i];
            }
            xMean = mean;
            double var = 0;
            for (int i = 0; i < indicator.getExpectations().length; i++) {
                var += Math.pow(indicator.getExpectations()[i] - xMean, 2) * indicator.getFractions()[i];
            }
            xVar = var;
        }
        final double weight = CorrelationFunction.sum(y.getExpectations())/indicator.getDocCount();
        if (weight > 1.0) {
            throw new AggregationExecutionException(
                "doc_count of indicator must be larger than the total count of the correlating values indicator count ["
                    + indicator.getDocCount()
                    + "] correlating value total count ["
                    + CorrelationFunction.sum(y.getExpectations())
                    + "]"
            );
        }
        final double yMean = weight;
        final double yVar = (1 - weight) * yMean * yMean + weight * (1 - yMean) * (1 - yMean);
        double xyCov = 0;
        if (indicator.getFractions() != null) {
            for (int i = 0; i < indicator.getExpectations().length; i++) {
                final double fraction = indicator.getFractions()[i];
                final double xVal = indicator.getExpectations()[i];
                final double nX = y.getExpectations()[i];
                xyCov = xyCov
                    - (indicator.getDocCount() * fraction - nX) * (xVal - xMean) * yMean
                    + nX * (xVal - xMean) * (1 - yMean);
            }
        } else {
            final double fraction = 1.0 / indicator.getExpectations().length;
            for (int i = 0; i < indicator.getExpectations().length; i++) {
                final double xVal = indicator.getExpectations()[i];
                final double nX = y.getExpectations()[i];
                xyCov = xyCov
                    - (indicator.getDocCount() * fraction - nX) * (xVal - xMean) * yMean
                    + nX * (xVal - xMean) * (1 - yMean);
            }
        }
        xyCov /= indicator.getDocCount();
        return (xVar * yVar == 0) ? Double.NaN : xyCov / Math.sqrt(xVar * yVar);
    }

    @Override
    public void validate(PipelineAggregationBuilder.ValidationContext context, String bucketPath) {
        if (bucketPath.endsWith("_count") == false) {
            context.addBucketPathValidationError("count correlation requires that bucket_path points to bucket [_count]");
        }
    }
}
