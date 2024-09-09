/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbedding;

import java.util.Objects;

public class TextEmbeddingModelValidator implements ModelValidator {

    private final ServiceIntegrationValidator serviceIntegrationValidator;

    public TextEmbeddingModelValidator(ServiceIntegrationValidator serviceIntegrationValidator) {
        this.serviceIntegrationValidator = serviceIntegrationValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, ActionListener<Model> listener) {
        serviceIntegrationValidator.validate(service, model, listener.delegateFailureAndWrap((delegate, r) -> {
            delegate.onResponse(postValidate(model, r));
        }));
    }

    private Model postValidate(Model model, InferenceServiceResults results) {
        if (results instanceof TextEmbedding embeddingResults) {
            try {
                if (model.getServiceSettings().dimensionsSetByUser()
                    && model.getServiceSettings().dimensions() != null
                    && Objects.equals(model.getServiceSettings().dimensions(), embeddingResults.getFirstEmbeddingSize()) == false) {
                    throw new ElasticsearchStatusException(
                        Strings.format(
                            "The retrieved embeddings size [%s] does not match the size specified in the settings [%s]. "
                                + "Please recreate the [%s] configuration with the correct dimensions",
                            embeddingResults.getFirstEmbeddingSize(),
                            model.getServiceSettings().dimensions(),
                            model.getConfigurations().getInferenceEntityId()
                        ),
                        RestStatus.BAD_REQUEST
                    );
                }
            } catch (Exception e) {
                throw new ElasticsearchStatusException(
                    "Could not determine embedding size. "
                        + "Expected a result of type ["
                        + InferenceTextEmbeddingFloatResults.NAME
                        + "] got ["
                        + results.getWriteableName()
                        + "]",
                    RestStatus.BAD_REQUEST
                );
            }

            var similarityFromModel = model.getServiceSettings().similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            ServiceSettings serviceSettings = model.getServiceSettings();
            serviceSettings.setSimilarity(similarityToUse);

            return model;
        } else {
            throw new ElasticsearchStatusException("Validation call did not return text embedding response", RestStatus.BAD_REQUEST);
        }
    }
}
