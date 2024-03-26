/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the cohere model type.
 */
public class CohereActionCreator implements CohereActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public CohereActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(CohereEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var overriddenModel = CohereEmbeddingsModel.of(model, taskSettings, inputType);

        return new CohereEmbeddingsAction(sender, overriddenModel);
    }
}
