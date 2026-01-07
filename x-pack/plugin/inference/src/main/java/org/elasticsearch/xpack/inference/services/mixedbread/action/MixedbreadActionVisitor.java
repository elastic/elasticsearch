/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;

import java.util.Map;

/**
 * Interface for creating {@link ExecutableAction} instances for Mixedbread models.
 * <p>
 * This interface is used to create {@link ExecutableAction} instances for Mixedbread models
 * {@link MixedbreadRerankModel}.
 */
public interface MixedbreadActionVisitor {

    /**
     * Creates an {@link ExecutableAction} for the given {@link MixedbreadRerankModel}.
     *
     * @param model The model to create the action for.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(MixedbreadRerankModel model, Map<String, Object> taskSettings);
}
