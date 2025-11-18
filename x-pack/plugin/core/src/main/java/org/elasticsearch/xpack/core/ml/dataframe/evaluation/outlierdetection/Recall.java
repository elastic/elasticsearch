/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

public class Recall extends AbstractConfusionMatrixMetric {

    public static final ParseField NAME = new ParseField("recall");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Recall, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new Recall((List<Double>) a[0])
    );

    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), AT);
    }

    public static Recall fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Recall(List<Double> at) {
        super(at);
    }

    public Recall(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(OutlierDetection.NAME, NAME);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Recall that = (Recall) o;
        return Arrays.equals(thresholds, that.thresholds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(thresholds);
    }

    @Override
    protected List<AggregationBuilder> aggsAt(String actualField, String predictedProbabilityField) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (int i = 0; i < thresholds.length; i++) {
            double threshold = thresholds[i];
            aggs.add(buildAgg(actualField, predictedProbabilityField, threshold, Condition.TP));
            aggs.add(buildAgg(actualField, predictedProbabilityField, threshold, Condition.FN));
        }
        return aggs;
    }

    @Override
    public EvaluationMetricResult evaluate(InternalAggregations aggs) {
        double[] recalls = new double[thresholds.length];
        for (int i = 0; i < thresholds.length; i++) {
            double threshold = thresholds[i];
            SingleBucketAggregation tpAgg = aggs.get(aggName(threshold, Condition.TP));
            SingleBucketAggregation fnAgg = aggs.get(aggName(threshold, Condition.FN));
            long tp = tpAgg.getDocCount();
            long fn = fnAgg.getDocCount();
            recalls[i] = tp + fn == 0 ? 0.0 : (double) tp / (tp + fn);
        }
        return new ScoreByThresholdResult(NAME.getPreferredName(), thresholds, recalls);
    }
}
