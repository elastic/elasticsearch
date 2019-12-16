/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING;

public class LocalModel implements Model {

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> fieldNames;

    public LocalModel(String modelId, TrainedModelDefinition trainedModelDefinition, TrainedModelInput input) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
        this.fieldNames = new HashSet<>(input.getFieldNames());
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
    public void infer(Map<String, Object> fields, InferenceConfig config, ActionListener<InferenceResults> listener) {
        try {
            if (Sets.haveEmptyIntersection(fieldNames, fields.keySet())) {
                listener.onResponse(new WarningInferenceResults(Messages.getMessage(INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId)));
                return;
            }

            listener.onResponse(trainedModelDefinition.infer(fields, config));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
