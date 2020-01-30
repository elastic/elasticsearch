/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.client.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Evaluation of binary soft classification methods, e.g. outlier detection.
 * This is useful to evaluate problems where a model outputs a probability of whether
 * a data frame row belongs to one of two groups.
 */
public class BinarySoftClassification implements Evaluation {

    public static final String NAME = "binary_soft_classification";

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_PROBABILITY_FIELD = new ParseField("predicted_probability_field");
    private static final ParseField METRICS = new ParseField("metrics");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BinarySoftClassification, Void> PARSER =
        new ConstructingObjectParser<>(
            NAME,
            true,
            args -> new BinarySoftClassification((String) args[0], (String) args[1], (List<EvaluationMetric>) args[2]));

    static {
        PARSER.declareString(constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(constructorArg(), PREDICTED_PROBABILITY_FIELD);
        PARSER.declareNamedObjects(
            optionalConstructorArg(), (p, c, n) -> p.namedObject(EvaluationMetric.class, registeredMetricName(NAME, n), null), METRICS);
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
    private final List<EvaluationMetric> metrics;

    public BinarySoftClassification(String actualField, String predictedField) {
        this(actualField, predictedField, (List<EvaluationMetric>)null);
    }

    public BinarySoftClassification(String actualField, String predictedProbabilityField, EvaluationMetric... metric) {
        this(actualField, predictedProbabilityField, Arrays.asList(metric));
    }

    public BinarySoftClassification(String actualField, String predictedProbabilityField,
                                    @Nullable List<EvaluationMetric> metrics) {
        this.actualField = Objects.requireNonNull(actualField);
        this.predictedProbabilityField = Objects.requireNonNull(predictedProbabilityField);
        if (metrics != null) {
            metrics.sort(Comparator.comparing(EvaluationMetric::getName));
        }
        this.metrics = metrics;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), actualField);
        builder.field(PREDICTED_PROBABILITY_FIELD.getPreferredName(), predictedProbabilityField);

        if (metrics != null) {
            builder.startObject(METRICS.getPreferredName());
            for (EvaluationMetric metric : metrics) {
                builder.field(metric.getName(), metric);
            }
            builder.endObject();
        }

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
}
