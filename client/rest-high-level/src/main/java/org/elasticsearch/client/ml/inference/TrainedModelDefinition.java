/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TrainedModelDefinition implements ToXContentObject {

    public static final String NAME = "trained_model_doc";

    public static final ParseField TRAINED_MODEL = new ParseField("trained_model");
    public static final ParseField PREPROCESSORS = new ParseField("preprocessors");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME,
            true,
            TrainedModelDefinition.Builder::new);
    static {
        PARSER.declareNamedObject(TrainedModelDefinition.Builder::setTrainedModel,
            (p, c, n) -> p.namedObject(TrainedModel.class, n, null),
            TRAINED_MODEL);
        PARSER.declareNamedObjects(TrainedModelDefinition.Builder::setPreProcessors,
            (p, c, n) -> p.namedObject(PreProcessor.class, n, null),
            (trainedModelDefBuilder) -> {/* Does not matter client side*/ },
            PREPROCESSORS);
    }

    public static TrainedModelDefinition.Builder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final TrainedModel trainedModel;
    private final List<PreProcessor> preProcessors;

    TrainedModelDefinition(TrainedModel trainedModel, List<PreProcessor> preProcessors) {
        this.trainedModel = trainedModel;
        this.preProcessors = preProcessors == null ? Collections.emptyList() : Collections.unmodifiableList(preProcessors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        NamedXContentObjectHelper.writeNamedObjects(builder,
            params,
            false,
            TRAINED_MODEL.getPreferredName(),
            Collections.singletonList(trainedModel));
        NamedXContentObjectHelper.writeNamedObjects(builder,
            params,
            true,
            PREPROCESSORS.getPreferredName(),
            preProcessors);
        builder.endObject();
        return builder;
    }

    public TrainedModel getTrainedModel() {
        return trainedModel;
    }

    public List<PreProcessor> getPreProcessors() {
        return preProcessors;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDefinition that = (TrainedModelDefinition) o;
        return Objects.equals(trainedModel, that.trainedModel) &&
            Objects.equals(preProcessors, that.preProcessors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModel, preProcessors);
    }

    public static class Builder {

        private List<PreProcessor> preProcessors;
        private TrainedModel trainedModel;

        public Builder setPreProcessors(List<PreProcessor> preProcessors) {
            this.preProcessors = preProcessors;
            return this;
        }

        public Builder setTrainedModel(TrainedModel trainedModel) {
            this.trainedModel = trainedModel;
            return this;
        }

        public TrainedModelDefinition build() {
            return new TrainedModelDefinition(this.trainedModel, this.preProcessors);
        }
    }

}
