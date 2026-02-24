/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.util.Objects;

public class AzureOpenAiEmbeddingsRequest extends AzureOpenAiRequest {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final InputType inputType;
    private final AzureOpenAiEmbeddingsModel model;

    public AzureOpenAiEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        InputType inputType,
        AzureOpenAiEmbeddingsModel model
    ) {
        super(Objects.requireNonNull(model), model.getTaskSettings(), createRequestEntity(input, inputType, model));
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.inputType = inputType;
        this.model = model;
    }

    private static String createRequestEntity(Truncator.TruncationResult input, InputType inputType, AzureOpenAiEmbeddingsModel model) {
        return Strings.toString(
            new AzureOpenAiEmbeddingsRequestEntity(
                input.input(),
                inputType,
                model.getTaskSettings().user(),
                model.getServiceSettings().dimensions(),
                model.getServiceSettings().dimensionsSetByUser()
            )
        );
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new AzureOpenAiEmbeddingsRequest(truncator, truncatedInput, inputType, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
