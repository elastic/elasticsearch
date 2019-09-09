/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Evaluation of regression results.
 */
public class Regression implements Evaluation {

    public static final ParseField NAME = new ParseField("regression");

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_FIELD = new ParseField("predicted_field");
    private static final ParseField METRICS = new ParseField("metrics");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Regression, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(), a -> new Regression((String) a[0], (String) a[1], (List<RegressionMetric>) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PREDICTED_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(RegressionMetric.class, n, c), METRICS);
    }

    public static Regression fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * The field containing the actual value
     * The value of this field is assumed to be numeric
     */
    private final String actualField;

    /**
     * The field containing the predicted value
     * The value of this field is assumed to be numeric
     */
    private final String predictedField;

    /**
     * The list of metrics to calculate
     */
    private final List<RegressionMetric> metrics;

    public Regression(String actualField, String predictedField, @Nullable List<RegressionMetric> metrics) {
        this.actualField = ExceptionsHelper.requireNonNull(actualField, ACTUAL_FIELD);
        this.predictedField = ExceptionsHelper.requireNonNull(predictedField, PREDICTED_FIELD);
        this.metrics = initMetrics(metrics);
    }

    public Regression(StreamInput in) throws IOException {
        this.actualField = in.readString();
        this.predictedField = in.readString();
        this.metrics = in.readNamedWriteableList(RegressionMetric.class);
    }

    private static List<RegressionMetric> initMetrics(@Nullable List<RegressionMetric> parsedMetrics) {
        List<RegressionMetric> metrics = parsedMetrics == null ? defaultMetrics() : parsedMetrics;
        if (metrics.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must have one or more metrics", NAME.getPreferredName());
        }
        Collections.sort(metrics, Comparator.comparing(RegressionMetric::getMetricName));
        return metrics;
    }

    private static List<RegressionMetric> defaultMetrics() {
        List<RegressionMetric> defaultMetrics = new ArrayList<>(2);
        defaultMetrics.add(new MeanSquaredError());
        defaultMetrics.add(new RSquared());
        return defaultMetrics;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public SearchSourceBuilder buildSearch(QueryBuilder queryBuilder) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.existsQuery(actualField))
            .filter(QueryBuilders.existsQuery(predictedField))
            .filter(queryBuilder);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(boolQuery);
        for (RegressionMetric metric : metrics) {
            List<AggregationBuilder> aggs = metric.aggs(actualField, predictedField);
            aggs.forEach(searchSourceBuilder::aggregation);
        }
        return searchSourceBuilder;
    }

    @Override
    public void evaluate(SearchResponse searchResponse, ActionListener<List<EvaluationMetricResult>> listener) {
        List<EvaluationMetricResult> results = new ArrayList<>(metrics.size());
        if (searchResponse.getHits().getTotalHits().value == 0) {
            listener.onFailure(ExceptionsHelper.badRequestException("No documents found containing both [{}, {}] fields",
                actualField,
                predictedField));
            return;
        }
        for (RegressionMetric metric : metrics) {
            results.add(metric.evaluate(searchResponse.getAggregations()));
        }
        listener.onResponse(results);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(actualField);
        out.writeString(predictedField);
        out.writeNamedWriteableList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), actualField);
        builder.field(PREDICTED_FIELD.getPreferredName(), predictedField);

        builder.startObject(METRICS.getPreferredName());
        for (RegressionMetric metric : metrics) {
            builder.field(metric.getWriteableName(), metric);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Regression that = (Regression) o;
        return Objects.equals(that.actualField, this.actualField)
            && Objects.equals(that.predictedField, this.predictedField)
            && Objects.equals(that.metrics, this.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualField, predictedField, metrics);
    }
}
