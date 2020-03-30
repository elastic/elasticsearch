/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING;

public class LocalModel<T extends InferenceConfig> implements Model<T> {

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> fieldNames;
    private final Map<String, String> defaultFieldMap;
    private final T inferenceConfig;

    public LocalModel(String modelId,
                      TrainedModelDefinition trainedModelDefinition,
                      TrainedModelInput input,
                      Map<String, String> defaultFieldMap,
                      T modelInferenceConfig) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
        this.fieldNames = new HashSet<>(input.getFieldNames());
        this.defaultFieldMap = defaultFieldMap == null ? null : new HashMap<>(defaultFieldMap);
        this.inferenceConfig = modelInferenceConfig;
    }

    long ramBytesUsed() {
        return trainedModelDefinition.ramBytesUsed();
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    @Override
    public String getResultsType() {
        switch (trainedModelDefinition.getTrainedModel().targetType()) {
            case CLASSIFICATION:
                return ClassificationInferenceResults.NAME;
            case REGRESSION:
                return RegressionInferenceResults.NAME;
            default:
                throw ExceptionsHelper.badRequestException("Model [{}] has unsupported target type [{}]",
                    modelId,
                    trainedModelDefinition.getTrainedModel().targetType());
        }
    }

    @Override
    public void infer(Map<String, Object> fields, InferenceConfigUpdate<T> update, ActionListener<InferenceResults> listener) {
        if (update.isSupported(this.inferenceConfig) == false) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "Model [{}] has inference config of type [{}] which is not supported by inference request of type [{}]",
                this.modelId,
                this.inferenceConfig.getName(),
                update.getName()));
            return;
        }
        try {
            Model.mapFieldsIfNecessary(fields, defaultFieldMap);
            if (fieldNames.stream().allMatch(f -> MapHelper.dig(f, fields) == null)) {
                listener.onResponse(new WarningInferenceResults(Messages.getMessage(INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId)));
                return;
            }

            listener.onResponse(trainedModelDefinition.infer(fields, update.apply(inferenceConfig)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
