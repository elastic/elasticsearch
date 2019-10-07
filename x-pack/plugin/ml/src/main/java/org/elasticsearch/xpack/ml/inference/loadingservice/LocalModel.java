/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceParams;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;

import java.util.Map;

public class LocalModel implements Model {

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;

    public LocalModel(String modelId, TrainedModelDefinition trainedModelDefinition) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
    }

    @Override
    public String getResultsType() {
        switch (trainedModelDefinition.getTrainedModel().targetType()) {
            case CLASSIFICATION:
                return ClassificationInferenceResults.RESULT_TYPE;
            case REGRESSION:
                return RegressionInferenceResults.RESULT_TYPE;
            default:
                throw ExceptionsHelper.badRequestException("Model [{}] has unsupported target type [{}]",
                    modelId,
                    trainedModelDefinition.getTrainedModel().targetType());
        }
    }

    @Override
    public void infer(Map<String, Object> fields, InferenceParams params, ActionListener<InferenceResults> listener) {
        try {
            listener.onResponse(trainedModelDefinition.infer(fields, params));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
