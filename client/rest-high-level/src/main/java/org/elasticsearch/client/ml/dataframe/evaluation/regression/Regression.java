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
package org.elasticsearch.client.ml.dataframe.evaluation.regression;

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

/**
 * Evaluation of regression results.
 */
public class Regression implements Evaluation {

    public static final String NAME = "regression";

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_FIELD = new ParseField("predicted_field");
    private static final ParseField METRICS = new ParseField("metrics");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Regression, Void> PARSER = new ConstructingObjectParser<>(
        NAME, true, a -> new Regression((String) a[0], (String) a[1], (List<EvaluationMetric>) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PREDICTED_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(EvaluationMetric.class, n, c), METRICS);
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
    private final List<EvaluationMetric> metrics;

    public Regression(String actualField, String predictedField) {
        this(actualField, predictedField, (List<EvaluationMetric>)null);
    }

    public Regression(String actualField, String predictedField, EvaluationMetric... metrics) {
        this(actualField, predictedField, Arrays.asList(metrics));
    }

    public Regression(String actualField, String predictedField, @Nullable List<EvaluationMetric> metrics) {
        this.actualField = Objects.requireNonNull(actualField);
        this.predictedField = Objects.requireNonNull(predictedField);
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
        builder.field(PREDICTED_FIELD.getPreferredName(), predictedField);

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
