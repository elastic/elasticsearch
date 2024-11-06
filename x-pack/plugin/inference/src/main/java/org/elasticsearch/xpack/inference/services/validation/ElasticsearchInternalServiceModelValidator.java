/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandEmbeddingModel;

public class ElasticsearchInternalServiceModelValidator implements ModelValidator {

    private final ModelValidator modelValidator;

    public ElasticsearchInternalServiceModelValidator(ModelValidator modelValidator) {
        this.modelValidator = modelValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, ActionListener<Model> listener) {
        var modelToValidate = model;
        if (model instanceof CustomElandEmbeddingModel esModel) {
            modelToValidate = new CustomElandEmbeddingModel(
                esModel.getServiceSettings().modelId(),
                esModel.getTaskType(),
                esModel.getConfigurations().getService(),
                esModel.getServiceSettings(),
                esModel.getConfigurations().getChunkingSettings()
            );
        }

        modelValidator.validate(
            service,
            modelToValidate,
            listener.delegateFailureAndWrap((delegate, r) -> { delegate.onResponse(model); })
        );
    }
}
