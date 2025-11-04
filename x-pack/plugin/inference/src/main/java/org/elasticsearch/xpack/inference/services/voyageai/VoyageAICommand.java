/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;

/**
 * Command pattern interface for VoyageAI operations.
 * This allows for decoupling the request for an operation from the object that performs it.
 * Commands can be queued, logged, undone, and support macro operations.
 */
public interface VoyageAICommand {
    
    /**
     * Executes the VoyageAI command.
     * 
     * @param inputs the inputs for the inference operation
     * @param timeout timeout for the operation
     * @param listener callback for handling the results
     */
    void execute(InferenceInputs inputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener);
    
    /**
     * Returns a description of this command for logging and debugging purposes.
     */
    String getDescription();
    
    /**
     * Returns the type of command (embeddings, rerank, etc.)
     */
    CommandType getCommandType();
    
    /**
     * Enum representing different types of VoyageAI commands.
     */
    enum CommandType {
        TEXT_EMBEDDINGS("text_embeddings"),
        MULTIMODAL_EMBEDDINGS("multimodal_embeddings"), 
        CONTEXTUAL_EMBEDDINGS("contextual_embeddings"),
        RERANK("rerank");
        
        private final String name;
        
        CommandType(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
        
        @Override
        public String toString() {
            return name;
        }
    }
}