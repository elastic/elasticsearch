/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;

public class ElasticsearchInternalServiceModelValidator implements ModelValidator {

    ModelValidator modelValidator;

    public ElasticsearchInternalServiceModelValidator(ModelValidator modelValidator) {
        this.modelValidator = modelValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<Model> listener) {
        service.start(model, timeout, ActionListener.wrap((modelDeploymentStarted) -> {
            if (modelDeploymentStarted) {
                try {
                    modelValidator.validate(service, model, timeout, listener.delegateResponse((l, exception) -> {
                        stopModelDeployment(service, model, l, exception);
                    }));
                } catch (Exception e) {
                    stopModelDeployment(service, model, listener, e);
                }
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException("Could not deploy model for inference endpoint", RestStatus.INTERNAL_SERVER_ERROR)
                );
            }
        }, listener::onFailure));
    }

    private void stopModelDeployment(InferenceService service, Model model, ActionListener<Model> listener, Exception e) {
        service.stop(
            model,
            ActionListener.wrap(
                (v) -> listener.onFailure(e),
                (ex) -> listener.onFailure(
                    new ElasticsearchStatusException(
                        "Model validation failed and model deployment could not be stopped",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        ex
                    )
                )
            )
        );
    }
}
