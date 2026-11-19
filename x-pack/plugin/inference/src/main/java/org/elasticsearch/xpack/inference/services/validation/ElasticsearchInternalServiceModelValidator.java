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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandEmbeddingModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticDeployedModel;

public class ElasticsearchInternalServiceModelValidator implements ModelValidator {

    private final ServiceIntegrationValidator serviceIntegrationValidator;

    public ElasticsearchInternalServiceModelValidator(ServiceIntegrationValidator serviceIntegrationValidator) {
        this.serviceIntegrationValidator = serviceIntegrationValidator;
    }

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<ModelValidationResult> listener) {
        if (requiresDeploymentValidation(model)) {
            // Deploy the model before running the validation inference request. Validation calls service.infer(),
            // which fails with "Model [...] must be deployed to use" if the deployment has not been started.
            // Fixes https://github.com/elastic/elasticsearch/issues/144871. For ElasticDeployedModel (an
            // existing deployment) start()/stop() are no-ops, so it's ok to call start().
            // Return deploymentStarted=true so the caller can skip starting the deployment a second time.
            service.start(model, timeout, listener.delegateFailureAndWrap((startedListener, ignored) -> {
                var stopOnFailure = startedListener.delegateResponse((l, e) -> stopModelDeployment(service, model, l, e));
                serviceIntegrationValidator.validate(
                    service,
                    model,
                    timeout,
                    stopOnFailure.delegateFailureAndWrap(
                        (l, results) -> l.onResponse(new ModelValidationResult(postValidate(service, model, results), true))
                    )
                );
            }));
        } else {
            listener.onResponse(new ModelValidationResult(model, false));
        }
    }

    private static boolean requiresDeploymentValidation(Model model) {
        return (model instanceof CustomElandEmbeddingModel || model instanceof ElasticDeployedModel)
            && model.getTaskType() == TaskType.TEXT_EMBEDDING;
    }

    private void stopModelDeployment(InferenceService service, Model model, ActionListener<ModelValidationResult> listener, Exception e) {
        service.stop(model, ActionListener.wrap(stopped -> listener.onFailure(e), stopException -> {
            stopException.addSuppressed(e);
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Model validation failed and model deployment could not be stopped",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    stopException
                )
            );
        }));
    }

    private Model postValidate(InferenceService service, Model model, InferenceServiceResults results) {
        if (results instanceof DenseEmbeddingResults<?> embeddingResults) {
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
                    + DenseEmbeddingFloatResults.NAME
                    + "] got ["
                    + (results == null ? "null" : results.getWriteableName())
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
    }

    private int getEmbeddingSize(DenseEmbeddingResults<?> embeddingResults) {
        int embeddingSize;
        try {
            embeddingSize = embeddingResults.getFirstEmbeddingSize();
        } catch (Exception e) {
            throw new ElasticsearchStatusException("Could not determine embedding size", RestStatus.BAD_REQUEST, e);
        }
        return embeddingSize;
    }
}
