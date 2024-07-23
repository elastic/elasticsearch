/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.alibabacloudsearch;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.Map;
import java.util.Objects;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the alibaba cloud search model type.
 */
public class AlibabaCloudSearchActionCreator implements AlibabaCloudSearchActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AlibabaCloudSearchActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var overriddenModel = AlibabaCloudSearchEmbeddingsModel.of(model, taskSettings, inputType);

        return new AlibabaCloudSearchEmbeddingsAction(sender, overriddenModel, serviceComponents);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchSparseModel model, Map<String, Object> taskSettings, InputType inputType) {
        var overriddenModel = AlibabaCloudSearchSparseModel.of(model, taskSettings, inputType);

        return new AlibabaCloudSearchSparseAction(sender, overriddenModel, serviceComponents);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AlibabaCloudSearchRerankModel.of(model, taskSettings);

        return new AlibabaCloudSearchRerankAction(sender, overriddenModel, serviceComponents);
    }
}
