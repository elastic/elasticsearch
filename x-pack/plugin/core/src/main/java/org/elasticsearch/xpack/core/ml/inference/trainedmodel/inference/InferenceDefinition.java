/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition.PREPROCESSORS;
import static org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition.TRAINED_MODEL;

public class InferenceDefinition {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(InferenceDefinition.class);

    public static final String NAME = "inference_model_definition";
    private final InferenceModel trainedModel;
    private final List<PreProcessor> preProcessors;
    private Map<String, String> decoderMap;

    private static final ObjectParser<InferenceDefinition.Builder, Void> PARSER = new ObjectParser<>(NAME,
        true,
        InferenceDefinition.Builder::new);
    static {
        PARSER.declareNamedObject(InferenceDefinition.Builder::setTrainedModel,
            (p, c, n) -> p.namedObject(InferenceModel.class, n, null),
            TRAINED_MODEL);
        PARSER.declareNamedObjects(InferenceDefinition.Builder::setPreProcessors,
            (p, c, n) -> p.namedObject(LenientlyParsedPreProcessor.class, n, null),
            (trainedModelDefBuilder) -> {},
            PREPROCESSORS);
    }

    public static InferenceDefinition fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public InferenceDefinition(InferenceModel trainedModel, List<PreProcessor> preProcessors) {
        this.trainedModel = ExceptionsHelper.requireNonNull(trainedModel, TRAINED_MODEL);
        this.preProcessors = preProcessors == null ? Collections.emptyList() : Collections.unmodifiableList(preProcessors);
    }

    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(trainedModel);
        size += RamUsageEstimator.sizeOfCollection(preProcessors);
        return size;
    }

    InferenceModel getTrainedModel() {
        return trainedModel;
    }

    private void preProcess(Map<String, Object> fields) {
        preProcessors.forEach(preProcessor -> preProcessor.process(fields));
    }

    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config) {
        preProcess(fields);
        if (config.requestingImportance() && trainedModel.supportsFeatureImportance() == false) {
            throw ExceptionsHelper.badRequestException(
                "Feature importance is not supported for the configured model of type [{}]",
                trainedModel.getName());
        }
        return trainedModel.infer(fields,
            config,
            config.requestingImportance() ? getDecoderMap() : Collections.emptyMap());
    }

    public TargetType getTargetType() {
        return this.trainedModel.targetType();
    }

    private Map<String, String> getDecoderMap() {
        if (decoderMap != null) {
            return decoderMap;
        }
        synchronized (this) {
            if (decoderMap != null) {
                return decoderMap;
            }
            this.decoderMap = preProcessors.stream()
                .filter(p -> p.isCustom() == false)
                .map(PreProcessor::reverseLookup)
                .collect(HashMap::new, Map::putAll, Map::putAll);
            return decoderMap;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private List<PreProcessor> preProcessors;
        private InferenceModel inferenceModel;

        public Builder setPreProcessors(List<PreProcessor> preProcessors) {
            this.preProcessors = preProcessors;
            return this;
        }

        public Builder setTrainedModel(InferenceModel trainedModel) {
            this.inferenceModel = trainedModel;
            return this;
        }

        public InferenceDefinition build() {
            this.inferenceModel.rewriteFeatureIndices(Collections.emptyMap());
            return new InferenceDefinition(this.inferenceModel, this.preProcessors);
        }
    }
}
