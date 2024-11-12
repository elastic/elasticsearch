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

public class ElasticsearchInternalServiceModelValidator implements ModelValidator {

    ModelValidator modelValidator;

    public ElasticsearchInternalServiceModelValidator(ModelValidator modelValidator) {
        this.modelValidator = modelValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, ActionListener<Model> listener) {
        modelValidator.validate(service, model, listener.delegateResponse((l, exception) -> {
            // TODO: Cleanup the below code
            service.stop(model, ActionListener.wrap((v) -> listener.onFailure(exception), (e) -> listener.onFailure(exception)));
        }));
    }
}
