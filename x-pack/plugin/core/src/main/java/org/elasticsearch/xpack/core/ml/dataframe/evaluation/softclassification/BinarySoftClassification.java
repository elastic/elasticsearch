/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

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
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Evaluation of binary soft classification methods, e.g. outlier detection.
 * This is useful to evaluate problems where a model outputs a probability of whether
 * a data frame row belongs to one of two groups.
 */
public class BinarySoftClassification implements Evaluation {

    public static final ParseField NAME = new ParseField("binary_soft_classification");

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_PROBABILITY_FIELD = new ParseField("predicted_probability_field");
    private static final ParseField METRICS = new ParseField("metrics");

    public static final ConstructingObjectParser<BinarySoftClassification, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(), a -> new BinarySoftClassification((String) a[0], (String) a[1], (List<SoftClassificationMetric>) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PREDICTED_PROBABILITY_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(SoftClassificationMetric.class, n, null), METRICS);
    }

    public static BinarySoftClassification fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * The field where the actual class is marked up.
     * The value of this field is assumed to either be 1 or 0, or true or false.
     */
    private final String actualField;

    /**
     * The field of the predicted probability in [0.0, 1.0].
     */
    private final String predictedProbabilityField;

    /**
     * The list of metrics to calculate
     */
    private final List<SoftClassificationMetric> metrics;

    public BinarySoftClassification(String actualField, String predictedProbabilityField,
                                    @Nullable List<SoftClassificationMetric> metrics) {
        this.actualField = ExceptionsHelper.requireNonNull(actualField, ACTUAL_FIELD);
        this.predictedProbabilityField = ExceptionsHelper.requireNonNull(predictedProbabilityField, PREDICTED_PROBABILITY_FIELD);
        this.metrics = initMetrics(metrics);
    }

    private static List<SoftClassificationMetric> initMetrics(@Nullable List<SoftClassificationMetric> parsedMetrics) {
        List<SoftClassificationMetric> metrics = parsedMetrics == null ? defaultMetrics() : parsedMetrics;
        if (metrics.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must have one or more metrics", NAME.getPreferredName());
        }
        Collections.sort(metrics, Comparator.comparing(SoftClassificationMetric::getMetricName));
        return metrics;
    }

    private static List<SoftClassificationMetric> defaultMetrics() {
        List<SoftClassificationMetric> defaultMetrics = new ArrayList<>(4);
        defaultMetrics.add(new AucRoc(false));
        defaultMetrics.add(new Precision(Arrays.asList(0.25, 0.5, 0.75)));
        defaultMetrics.add(new Recall(Arrays.asList(0.25, 0.5, 0.75)));
        defaultMetrics.add(new ConfusionMatrix(Arrays.asList(0.25, 0.5, 0.75)));
        return defaultMetrics;
    }

    public BinarySoftClassification(StreamInput in) throws IOException {
        this.actualField = in.readString();
        this.predictedProbabilityField = in.readString();
        this.metrics = in.readNamedWriteableList(SoftClassificationMetric.class);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(actualField);
        out.writeString(predictedProbabilityField);
        out.writeNamedWriteableList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), actualField);
        builder.field(PREDICTED_PROBABILITY_FIELD.getPreferredName(), predictedProbabilityField);

        builder.startObject(METRICS.getPreferredName());
        for (SoftClassificationMetric metric : metrics) {
            builder.field(metric.getMetricName(), metric);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinarySoftClassification that = (BinarySoftClassification) o;
        return Objects.equals(actualField, that.actualField)
            && Objects.equals(predictedProbabilityField, that.predictedProbabilityField)
            && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualField, predictedProbabilityField, metrics);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public SearchSourceBuilder buildSearch(QueryBuilder queryBuilder) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.existsQuery(actualField))
            .filter(QueryBuilders.existsQuery(predictedProbabilityField))
            .filter(queryBuilder);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(boolQuery);
        for (SoftClassificationMetric metric : metrics) {
            List<AggregationBuilder> aggs = metric.aggs(actualField, Collections.singletonList(new BinaryClassInfo()));
            aggs.forEach(searchSourceBuilder::aggregation);
        }
        return searchSourceBuilder;
    }

    @Override
    public void evaluate(SearchResponse searchResponse, ActionListener<List<EvaluationMetricResult>> listener) {
        if (searchResponse.getHits().getTotalHits().value == 0) {
            listener.onFailure(ExceptionsHelper.badRequestException("No documents found containing both [{}, {}] fields", actualField,
                predictedProbabilityField));
            return;
        }

        List<EvaluationMetricResult> results = new ArrayList<>();
        Aggregations aggs = searchResponse.getAggregations();
        BinaryClassInfo binaryClassInfo = new BinaryClassInfo();
        for (SoftClassificationMetric metric : metrics) {
            results.add(metric.evaluate(binaryClassInfo, aggs));
        }
        listener.onResponse(results);
    }

    private class BinaryClassInfo implements SoftClassificationMetric.ClassInfo {

        private QueryBuilder matchingQuery = QueryBuilders.queryStringQuery(actualField + ": (1 OR true)");

        @Override
        public String getName() {
            return String.valueOf(true);
        }

        @Override
        public QueryBuilder matchingQuery() {
            return matchingQuery;
        }

        @Override
        public String getProbabilityField() {
            return predictedProbabilityField;
        }
    }
}
