/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;

public class SimpleModelValidator implements ModelValidator {

    private final ServiceIntegrationValidator serviceIntegrationValidator;

    public SimpleModelValidator(ServiceIntegrationValidator serviceIntegrationValidator) {
        this.serviceIntegrationValidator = serviceIntegrationValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<Model> listener) {
        serviceIntegrationValidator.validate(service, model, timeout, listener.delegateFailureAndWrap((delegate, r) -> {
            delegate.onResponse(model);
        }));
    }
}
