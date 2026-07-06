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

/**
 * This class is slightly different from the SimpleServiceIntegrationValidator in that in sends the topN and return documents in the
 * request. This is necessary because the custom service may require those template to be replaced when building the request. Otherwise,
 * the request will fail to be constructed because it'll have a template that wasn't replaced.
 */
public class CustomServiceRerankIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_INPUT = List.of("how big");
    private static final String QUERY = "test query";

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.rerankInfer(
            model,
            new RerankRequest(InferenceString.fromStringList(TEST_INPUT), InferenceString.ofText(QUERY), 1, true, null),
            timeout,
            ServiceIntegrationValidator.wrapListenerForValidation(listener)
        );
    }
}
