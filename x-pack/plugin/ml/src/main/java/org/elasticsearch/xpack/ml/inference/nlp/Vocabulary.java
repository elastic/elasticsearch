/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Vocabulary implements Writeable, ToXContentObject {

    private static final String NAME = "vocabulary";
    public static final ParseField VOCABULARY = new ParseField(NAME);
    public static final ParseField MERGES = new ParseField("merges");
    public static final ParseField SCORES = new ParseField("scores");

    @SuppressWarnings({ "unchecked" })
    public static ConstructingObjectParser<Vocabulary, Void> createParser(boolean ignoreUnkownFields) {
        ConstructingObjectParser<Vocabulary, Void> parser = new ConstructingObjectParser<>(
            "vocabulary",
            ignoreUnkownFields,
            a -> new Vocabulary((List<String>) a[0], (String) a[1], (List<String>) a[2], (List<Double>) a[3])
        );
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), VOCABULARY);
        parser.declareString(ConstructingObjectParser.constructorArg(), TrainedModelConfig.MODEL_ID);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), MERGES);
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), SCORES);
        return parser;
    }

    public static final ConstructingObjectParser<Vocabulary, Void> PARSER = createParser(true);

    private final List<String> vocab;
    private final List<String> merges;
    private final List<Double> scores;
    private final String modelId;

    public Vocabulary(List<String> vocab, String modelId, @Nullable List<String> merges, @Nullable List<Double> scores) {
        this.vocab = ExceptionsHelper.requireNonNull(vocab, VOCABULARY);
        this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
        this.merges = Optional.ofNullable(merges).orElse(List.of());
        this.scores = Optional.ofNullable(scores).orElse(List.of());
    }

    public Vocabulary(StreamInput in) throws IOException {
        vocab = in.readStringCollectionAsList();
        modelId = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            merges = in.readStringCollectionAsList();
        } else {
            merges = List.of();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            scores = in.readCollectionAsList(StreamInput::readDouble);
        } else {
            scores = List.of();
        }
    }

    public List<String> get() {
        return vocab;
    }

    public List<String> merges() {
        return merges;
    }

    public List<Double> scores() {
        return scores;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(vocab);
        out.writeString(modelId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            out.writeStringCollection(merges);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeCollection(scores, StreamOutput::writeDouble);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vocabulary that = (Vocabulary) o;
        return Objects.equals(vocab, that.vocab)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(merges, that.merges)
            && Objects.equals(scores, that.scores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocab, modelId, merges, scores);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocab);
        builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        }
        if (merges.isEmpty() == false) {
            builder.field(MERGES.getPreferredName(), merges);
        }
        if (scores.isEmpty() == false) {
            builder.field(SCORES.getPreferredName(), scores);
        }
        builder.endObject();
        return builder;
    }
}
