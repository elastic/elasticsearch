/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractConfusionMatrixMetric implements SoftClassificationMetric {

    public static final ParseField AT = new ParseField("at");

    protected final double[] thresholds;

    protected AbstractConfusionMatrixMetric(double[] thresholds) {
        this.thresholds = ExceptionsHelper.requireNonNull(thresholds, AT);
        if (thresholds.length == 0) {
            throw ExceptionsHelper.badRequestException("[" + getMetricName() + "." + AT.getPreferredName()
                + "] must have at least one value");
        }
        for (double threshold : thresholds) {
            if (threshold < 0 || threshold > 1.0) {
                throw ExceptionsHelper.badRequestException("[" + getMetricName() + "." + AT.getPreferredName()
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
    public final List<AggregationBuilder> aggs(String actualField, List<ClassInfo> classInfos) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (double threshold : thresholds) {
            aggs.addAll(aggsAt(actualField, classInfos, threshold));
        }
        return aggs;
    }

    protected abstract List<AggregationBuilder> aggsAt(String labelField, List<ClassInfo> classInfos, double threshold);

    protected enum Condition {
        TP, FP, TN, FN;
    }

    protected String aggName(ClassInfo classInfo, double threshold, Condition condition) {
        return getMetricName() + "_" + classInfo.getName() + "_at_" + threshold + "_" + condition.name();
    }

    protected AggregationBuilder buildAgg(ClassInfo classInfo, double threshold, Condition condition) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        switch (condition) {
            case TP:
                boolQuery.must(classInfo.matchingQuery());
                boolQuery.must(QueryBuilders.rangeQuery(classInfo.getProbabilityField()).gte(threshold));
                break;
            case FP:
                boolQuery.mustNot(classInfo.matchingQuery());
                boolQuery.must(QueryBuilders.rangeQuery(classInfo.getProbabilityField()).gte(threshold));
                break;
            case TN:
                boolQuery.mustNot(classInfo.matchingQuery());
                boolQuery.must(QueryBuilders.rangeQuery(classInfo.getProbabilityField()).lt(threshold));
                break;
            case FN:
                boolQuery.must(classInfo.matchingQuery());
                boolQuery.must(QueryBuilders.rangeQuery(classInfo.getProbabilityField()).lt(threshold));
                break;
            default:
                throw new IllegalArgumentException("Unknown enum value: " + condition);
        }
        return AggregationBuilders.filter(aggName(classInfo, threshold, condition), boolQuery);
    }
}
