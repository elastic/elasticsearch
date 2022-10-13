/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

public class Precision extends AbstractConfusionMatrixMetric {

    public static final ParseField NAME = new ParseField("precision");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Precision, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new Precision((List<Double>) a[0])
    );

    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), AT);
    }

    public static Precision fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Precision(List<Double> at) {
        super(at);
    }

    public Precision(StreamInput in) throws IOException {
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
        Precision that = (Precision) o;
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
            aggs.add(buildAgg(actualField, predictedProbabilityField, threshold, Condition.FP));
        }
        return aggs;
    }

    @Override
    public EvaluationMetricResult evaluate(Aggregations aggs) {
        double[] precisions = new double[thresholds.length];
        for (int i = 0; i < thresholds.length; i++) {
            double threshold = thresholds[i];
            Filter tpAgg = aggs.get(aggName(threshold, Condition.TP));
            Filter fpAgg = aggs.get(aggName(threshold, Condition.FP));
            long tp = tpAgg.getDocCount();
            long fp = fpAgg.getDocCount();
            precisions[i] = tp + fp == 0 ? 0.0 : (double) tp / (tp + fp);
        }
        return new ScoreByThresholdResult(NAME.getPreferredName(), thresholds, precisions);
    }
}
