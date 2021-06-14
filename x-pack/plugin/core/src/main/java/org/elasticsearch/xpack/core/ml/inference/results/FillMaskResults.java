/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FillMaskResults implements InferenceResults {

    public static final String NAME = "fill_mask_result";
    public static final String DEFAULT_RESULTS_FIELD = "results";

    private final List<Prediction> predictions;

    public FillMaskResults(List<Prediction> predictions) {
        this.predictions = predictions;
    }

    public FillMaskResults(StreamInput in) throws IOException {
        this.predictions = in.readList(Prediction::new);
    }

    public List<Prediction> getPredictions() {
        return predictions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(DEFAULT_RESULTS_FIELD);
        for (Prediction prediction : predictions) {
            prediction.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(predictions);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(DEFAULT_RESULTS_FIELD, predictions.stream().map(Prediction::toMap).collect(Collectors.toList()));
        return map;
    }

    @Override
    public Object predictedValue() {
        if (predictions.isEmpty()) {
            return null;
        }
        return predictions.get(0).token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FillMaskResults that = (FillMaskResults) o;
        return Objects.equals(predictions, that.predictions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predictions);
    }

    public static class Prediction implements ToXContentObject, Writeable {

        private static final ParseField TOKEN = new ParseField("token");
        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField SEQUENCE = new ParseField("sequence");

        private final String token;
        private final double score;
        private final String sequence;

        public Prediction(String token, double score, String sequence) {
            this.token = Objects.requireNonNull(token);
            this.score = score;
            this.sequence = Objects.requireNonNull(sequence);
        }

        public Prediction(StreamInput in) throws IOException {
            token = in.readString();
            score = in.readDouble();
            sequence = in.readString();
        }

        public double getScore() {
            return score;
        }

        public String getSequence() {
            return sequence;
        }

        public String getToken() {
            return token;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(TOKEN.getPreferredName(), token);
            map.put(SCORE.getPreferredName(), score);
            map.put(SEQUENCE.getPreferredName(), sequence);
            return map;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOKEN.getPreferredName(), token);
            builder.field(SCORE.getPreferredName(), score);
            builder.field(SEQUENCE.getPreferredName(), sequence);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(token);
            out.writeDouble(score);
            out.writeString(sequence);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Prediction result = (Prediction) o;
            return Double.compare(result.score, score) == 0 &&
                Objects.equals(token, result.token) &&
                Objects.equals(sequence, result.sequence);
        }

        @Override
        public int hashCode() {
            return Objects.hash(token, score, sequence);
        }
    }
}
