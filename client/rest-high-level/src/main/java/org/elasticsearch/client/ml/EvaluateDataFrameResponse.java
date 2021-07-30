/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class EvaluateDataFrameResponse implements ToXContentObject {

    public static EvaluateDataFrameResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        String evaluationName = parser.currentName();
        parser.nextToken();
        Map<String, EvaluationMetric.Result> metrics = parser.map(LinkedHashMap::new, p -> parseMetric(evaluationName, p));
        List<EvaluationMetric.Result> knownMetrics =
            metrics.values().stream()
                .filter(Objects::nonNull)  // Filter out null values returned by {@link EvaluateDataFrameResponse::parseMetric}.
                .collect(Collectors.toList());
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new EvaluateDataFrameResponse(evaluationName, knownMetrics);
    }

    private static EvaluationMetric.Result parseMetric(String evaluationName, XContentParser parser) throws IOException {
        String metricName = parser.currentName();
        try {
            return parser.namedObject(EvaluationMetric.Result.class, registeredMetricName(evaluationName, metricName), null);
        } catch (NamedObjectNotFoundException e) {
            parser.skipChildren();
            // Metric name not recognized. Return {@code null} value here and filter it out later.
            return null;
        }
    }

    private final String evaluationName;
    private final Map<String, EvaluationMetric.Result> metrics;

    public EvaluateDataFrameResponse(String evaluationName, List<EvaluationMetric.Result> metrics) {
        this.evaluationName = Objects.requireNonNull(evaluationName);
        this.metrics = Objects.requireNonNull(metrics).stream().collect(Collectors.toUnmodifiableMap(m -> m.getMetricName(), m -> m));
    }

    public String getEvaluationName() {
        return evaluationName;
    }

    public List<EvaluationMetric.Result> getMetrics() {
        return metrics.values().stream().collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public <T extends EvaluationMetric.Result> T getMetricByName(String metricName) {
        Objects.requireNonNull(metricName);
        return (T) metrics.get(metricName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder
            .startObject()
            .field(evaluationName, metrics)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvaluateDataFrameResponse that = (EvaluateDataFrameResponse) o;
        return Objects.equals(evaluationName, that.evaluationName)
            && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evaluationName, metrics);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
