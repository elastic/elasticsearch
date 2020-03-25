/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.BinarySoftClassification.actualIsTrueQuery;

abstract class AbstractConfusionMatrixMetric implements EvaluationMetric {

    public static final ParseField AT = new ParseField("at");

    protected final double[] thresholds;
    private EvaluationMetricResult result;

    protected AbstractConfusionMatrixMetric(List<Double> at) {
        this.thresholds = ExceptionsHelper.requireNonNull(at, AT).stream().mapToDouble(Double::doubleValue).toArray();
        if (thresholds.length == 0) {
            throw ExceptionsHelper.badRequestException("[" + getName() + "." + AT.getPreferredName() + "] must have at least one value");
        }
        for (double threshold : thresholds) {
            if (threshold < 0 || threshold > 1.0) {
                throw ExceptionsHelper.badRequestException("[" + getName() + "." + AT.getPreferredName()
                    + "] values must be in [0.0, 1.0]");
            }
        }
    }

    protected AbstractConfusionMatrixMetric(StreamInput in) throws IOException {
        this.thresholds = in.readDoubleArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(thresholds);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AT.getPreferredName(), thresholds);
        builder.endObject();
        return builder;
    }

    @Override
    public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                  String actualField,
                                                                                  String predictedProbabilityField) {
        if (result != null) {
            return Tuple.tuple(List.of(), List.of());
        }
        return Tuple.tuple(aggsAt(actualField, predictedProbabilityField), List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        result = evaluate(aggs);
    }

    @Override
    public Optional<EvaluationMetricResult> getResult() {
        return Optional.ofNullable(result);
    }

    protected abstract List<AggregationBuilder> aggsAt(String actualField, String predictedProbabilityField);

    protected abstract EvaluationMetricResult evaluate(Aggregations aggs);

    enum Condition {
        TP(true, true),
        FP(false, true),
        TN(false, false),
        FN(true, false);

        final boolean actual;
        final boolean predicted;

        Condition(boolean actual, boolean predicted) {
            this.actual = actual;
            this.predicted = predicted;
        }
    }

    protected String aggName(double threshold, Condition condition) {
        return getName() + "_at_" + threshold + "_" + condition.name();
    }

    protected AggregationBuilder buildAgg(String actualField, String predictedProbabilityField, double threshold, Condition condition) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        QueryBuilder actualIsTrueQuery = actualIsTrueQuery(actualField);
        QueryBuilder predictedIsTrueQuery = QueryBuilders.rangeQuery(predictedProbabilityField).gte(threshold);
        if (condition.actual) {
            boolQuery.must(actualIsTrueQuery);
        } else {
            boolQuery.mustNot(actualIsTrueQuery);
        }
        if (condition.predicted) {
            boolQuery.must(predictedIsTrueQuery);
        } else {
            boolQuery.mustNot(predictedIsTrueQuery);
        }
        return AggregationBuilders.filter(aggName(threshold, condition), boolQuery);
    }
}
