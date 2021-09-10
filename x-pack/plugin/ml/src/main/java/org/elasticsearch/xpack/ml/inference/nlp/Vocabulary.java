/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Vocabulary implements Writeable, ToXContentObject {

    private static final String NAME = "vocabulary";
    private static final ParseField VOCAB = new ParseField("vocab");

    @SuppressWarnings({ "unchecked"})
    public static ConstructingObjectParser<Vocabulary, Void> createParser(boolean ignoreUnkownFields) {
        ConstructingObjectParser<Vocabulary, Void> parser = new ConstructingObjectParser<>("vocabulary", ignoreUnkownFields,
            a -> new Vocabulary((List<String>) a[0], (String) a[1]));
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), VOCAB);
        parser.declareString(ConstructingObjectParser.constructorArg(), TrainedModelConfig.MODEL_ID);
        return parser;
    }

    private final List<String> vocab;
    private final String modelId;

    public Vocabulary(List<String> vocab, String modelId) {
        this.vocab = ExceptionsHelper.requireNonNull(vocab, VOCAB);
        this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
    }

    public Vocabulary(StreamInput in) throws IOException {
        vocab = in.readStringList();
        modelId = in.readString();
    }

    public List<String> get() {
        return vocab;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(vocab);
        out.writeString(modelId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vocabulary that = (Vocabulary) o;
        return Objects.equals(vocab, that.vocab) && Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocab, modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCAB.getPreferredName(), vocab);
        builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        }
        builder.endObject();
        return builder;
    }
}
