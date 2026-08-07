
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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;

import java.util.List;

public class RerankServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_INPUT = List.of("how big");
    private static final String QUERY = "test query";

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.rerankInfer(
            model,
            new RerankRequest(InferenceString.fromStringList(TEST_INPUT), InferenceString.ofText(QUERY), null, null, null),
            timeout,
            ServiceIntegrationValidator.wrapListenerForValidation(listener)
        );
    }

}
