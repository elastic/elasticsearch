/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutTrainedModelVocabularyAction extends ActionType<AcknowledgedResponse> {

    public static final PutTrainedModelVocabularyAction INSTANCE = new PutTrainedModelVocabularyAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/vocabulary/put";

    private PutTrainedModelVocabularyAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField VOCABULARY = new ParseField("vocabulary");
        public static final ParseField MERGES = new ParseField("merges");
        public static final ParseField SCORES = new ParseField("scores");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("put_trained_model_vocabulary", Builder::new);
        static {
            PARSER.declareStringArray(Builder::setVocabulary, VOCABULARY);
            PARSER.declareStringArray(Builder::setMerges, MERGES);
            PARSER.declareDoubleArray(Builder::setScores, SCORES);
        }

        public static Request parseRequest(String modelId, XContentParser parser) {
            return PARSER.apply(parser, null).build(modelId);
        }

        private final String modelId;
        private final List<String> vocabulary;
        private final List<String> merges;
        private final List<Double> scores;

        public Request(String modelId, List<String> vocabulary, @Nullable List<String> merges, @Nullable List<Double> scores) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.vocabulary = ExceptionsHelper.requireNonNull(vocabulary, VOCABULARY);
            this.merges = Optional.ofNullable(merges).orElse(List.of());
            this.scores = Optional.ofNullable(scores).orElse(List.of());
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.vocabulary = in.readStringList();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
                this.merges = in.readStringList();
            } else {
                this.merges = List.of();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_009)) {
                this.scores = in.readList(StreamInput::readDouble);
            } else {
                this.scores = List.of();
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (vocabulary.isEmpty()) {
                validationException = addValidationError("[vocabulary] must not be empty", validationException);
            } else {
                if (scores.isEmpty() == false && scores.size() != vocabulary.size()) {
                    validationException = addValidationError("[scores] must have same length as [vocabulary]", validationException);
                }
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelId, request.modelId)
                && Objects.equals(vocabulary, request.vocabulary)
                && Objects.equals(scores, request.scores)
                && Objects.equals(merges, request.merges);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, vocabulary, merges, scores);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeStringCollection(vocabulary);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
                out.writeStringCollection(merges);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_009)) {
                out.writeCollection(scores, StreamOutput::writeDouble);
            }
        }

        public String getModelId() {
            return modelId;
        }

        public List<String> getVocabulary() {
            return vocabulary;
        }

        public List<String> getMerges() {
            return merges;
        }

        public List<Double> getScores() {
            return scores;
        }

        public static class Builder {
            private List<String> vocabulary;
            private List<String> merges;
            private List<Double> scores;

            public Builder setVocabulary(List<String> vocabulary) {
                this.vocabulary = vocabulary;
                return this;
            }

            public Builder setMerges(List<String> merges) {
                this.merges = merges;
                return this;
            }

            public Builder setScores(List<Double> scores) {
                this.scores = scores;
                return this;
            }

            public Request build(String modelId) {
                return new Request(modelId, vocabulary, merges, scores);
            }
        }
    }

}
