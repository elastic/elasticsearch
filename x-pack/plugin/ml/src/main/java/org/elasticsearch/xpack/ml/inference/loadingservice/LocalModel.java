/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalModel implements Model {

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> expectedInputFields;

    public LocalModel(String modelId, List<String> inputFields, TrainedModelDefinition trainedModelDefinition) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
        this.expectedInputFields = new HashSet<>(inputFields);
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
    public void infer(Map<String, Object> fields,
                      InferenceConfig config,
                      boolean allowMissingFields,
                      ActionListener<InferenceResults> listener) {
        try {
            if (allowMissingFields == false) {
                Set<String> missingFields = Sets.difference(expectedInputFields, fields.keySet());
                if (missingFields.isEmpty() == false) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(
                            "Not all expected fields for model [{}] have been supplied. Missing fields {}.",
                            modelId,
                            missingFields));
                    return;
                }
            }
            listener.onResponse(trainedModelDefinition.infer(fields, config));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
