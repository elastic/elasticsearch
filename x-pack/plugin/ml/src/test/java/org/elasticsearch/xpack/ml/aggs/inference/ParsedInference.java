/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.inference;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults.PREDICTION_PROBABILITY;
import static org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults.PREDICTION_SCORE;
import static org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults.FEATURE_IMPORTANCE;

/**
 * There isn't enough information in toXContent representation of the
 * {@link org.elasticsearch.xpack.core.ml.inference.results.InferenceResults}
 * objects to fully reconstruct them. In particular, depending on which
 * fields are written (result value, feature importance) it is not possible to
 * distinguish between a Regression result and a Classification result.
 *
 * This class parses the union all possible fields that may be written by
 * InferenceResults.
 *
 * The warning field is mutually exclusive with all the other fields.
 */
public class ParsedInference extends ParsedAggregation {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ParsedInference, Void> PARSER = new ConstructingObjectParser<>(
        ParsedInference.class.getSimpleName(),
        true,
        args -> new ParsedInference(
            args[0],
            (List<Map<String, Object>>) args[1],
            (List<TopClassEntry>) args[2],
            (String) args[3],
            (Double) args[4],
            (Double) args[5]
        )
    );

    static {
        PARSER.declareField(optionalConstructorArg(), (p, n) -> {
            Object o;
            XContentParser.Token token = p.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                o = p.text();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                o = p.booleanValue();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                o = p.doubleValue();
            } else {
                throw new XContentParseException(
                    p.getTokenLocation(),
                    "["
                        + ParsedInference.class.getSimpleName()
                        + "] failed to parse field ["
                        + CommonFields.VALUE
                        + "] "
                        + "value ["
                        + token
                        + "] is not a string, boolean or number"
                );
            }
            return o;
        }, CommonFields.VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> p.map(),
            new ParseField(SingleValueInferenceResults.FEATURE_IMPORTANCE)
        );
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> TopClassEntry.fromXContent(p),
            new ParseField(ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD)
        );
        PARSER.declareString(optionalConstructorArg(), new ParseField(WarningInferenceResults.NAME));
        PARSER.declareDouble(optionalConstructorArg(), new ParseField(PREDICTION_PROBABILITY));
        PARSER.declareDouble(optionalConstructorArg(), new ParseField(PREDICTION_SCORE));
        declareAggregationFields(PARSER);
    }

    public static ParsedInference fromXContent(XContentParser parser, final String name) {
        ParsedInference parsed = PARSER.apply(parser, null);
        parsed.setName(name);
        return parsed;
    }

    private final Object value;
    private final List<Map<String, Object>> featureImportance;
    private final List<TopClassEntry> topClasses;
    private final String warning;
    private final Double predictionProbability;
    private final Double predictionScore;

    ParsedInference(
        Object value,
        List<Map<String, Object>> featureImportance,
        List<TopClassEntry> topClasses,
        String warning,
        Double predictionProbability,
        Double predictionScore
    ) {
        this.value = value;
        this.warning = warning;
        this.featureImportance = featureImportance;
        this.topClasses = topClasses;
        this.predictionProbability = predictionProbability;
        this.predictionScore = predictionScore;
    }

    public Object getValue() {
        return value;
    }

    public List<Map<String, Object>> getFeatureImportance() {
        return featureImportance;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    public String getWarning() {
        return warning;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (warning != null) {
            builder.field(WarningInferenceResults.WARNING.getPreferredName(), warning);
        } else {
            builder.field(CommonFields.VALUE.getPreferredName(), value);
            if (topClasses != null && topClasses.size() > 0) {
                builder.field(ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD, topClasses);
            }
            if (featureImportance != null && featureImportance.size() > 0) {
                builder.field(FEATURE_IMPORTANCE, featureImportance);
            }
            if (predictionProbability != null) {
                builder.field(PREDICTION_PROBABILITY, predictionProbability);
            }
            if (predictionScore != null) {
                builder.field(PREDICTION_SCORE, predictionScore);
            }
        }
        return builder;
    }

    @Override
    public String getType() {
        return InferencePipelineAggregationBuilder.NAME;
    }
}
