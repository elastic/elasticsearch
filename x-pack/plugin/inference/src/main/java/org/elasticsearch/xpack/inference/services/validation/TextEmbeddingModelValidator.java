/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

public class TextEmbeddingModelValidator implements ModelValidator {

    private final ServiceIntegrationValidator serviceIntegrationValidator;

    public TextEmbeddingModelValidator(ServiceIntegrationValidator serviceIntegrationValidator) {
        this.serviceIntegrationValidator = serviceIntegrationValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<Model> listener) {
        serviceIntegrationValidator.validate(service, model, timeout, listener.delegateFailureAndWrap((delegate, r) -> {
            delegate.onResponse(postValidate(service, model, r));
        }));
    }

    private Model postValidate(InferenceService service, Model model, InferenceServiceResults results) {
        if (results instanceof TextEmbeddingResults<?> embeddingResults) {
            var serviceSettings = model.getServiceSettings();
            var dimensions = serviceSettings.dimensions();
            int embeddingSize = getEmbeddingSize(embeddingResults);

            if (Boolean.TRUE.equals(serviceSettings.dimensionsSetByUser())
                && dimensions != null
                && (dimensions.equals(embeddingSize) == false)) {
                throw new ElasticsearchStatusException(
                    Strings.format(
                        "The retrieved embeddings size [%s] does not match the size specified in the settings [%s]. "
                            + "Please recreate the [%s] configuration with the correct dimensions",
                        embeddingResults.getFirstEmbeddingSize(),
                        serviceSettings.dimensions(),
                        model.getInferenceEntityId()
                    ),
                    RestStatus.BAD_REQUEST
                );
            }

            return service.updateModelWithEmbeddingDetails(model, embeddingSize);
        } else {
            throw new ElasticsearchStatusException(
                "Validation call did not return expected results type."
                    + "Expected a result of type ["
                    + TextEmbeddingFloatResults.NAME
                    + "] got ["
                    + (results == null ? "null" : results.getWriteableName())
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
    }

    private int getEmbeddingSize(TextEmbeddingResults<?> embeddingResults) {
        int embeddingSize;
        try {
            embeddingSize = embeddingResults.getFirstEmbeddingSize();
        } catch (Exception e) {
            throw new ElasticsearchStatusException("Could not determine embedding size", RestStatus.BAD_REQUEST, e);
        }
        return embeddingSize;
    }
}
