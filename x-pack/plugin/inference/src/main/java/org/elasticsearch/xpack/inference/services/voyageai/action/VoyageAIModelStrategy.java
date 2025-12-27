/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import java.util.Map;

/**
 * Strategy interface for handling different VoyageAI model types.
 * Each strategy knows how to create the appropriate model and request manager.
 */
public interface VoyageAIModelStrategy<T extends VoyageAIModel> {
    
    /**
     * Creates an overridden model with the provided task settings.
     */
    T createOverriddenModel(T model, Map<String, Object> taskSettings);
    
    /**
     * Creates the appropriate request manager for this model type.
     */
    RequestManager createRequestManager(T model, ThreadPool threadPool);
    
    /**
     * Returns the service name for error messages.
     */
    String getServiceName();
}