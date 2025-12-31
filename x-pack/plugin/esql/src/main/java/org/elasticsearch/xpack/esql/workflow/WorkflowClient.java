/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.workflow;

/**
 * Client interface for executing Kibana workflows synchronously.
 * POC implementation - production would use proper service-to-service communication.
 */
public interface WorkflowClient {

    /**
     * Execute a workflow synchronously and return its JSON output.
     *
     * @param workflowId The ID of the workflow to execute
     * @param inputsJson JSON object containing the workflow inputs
     * @return JSON string containing the workflow output
     * @throws WorkflowExecutionException if the workflow execution fails
     */
    String executeWorkflowSync(String workflowId, String inputsJson) throws WorkflowExecutionException;

    /**
     * Exception thrown when workflow execution fails.
     */
    class WorkflowExecutionException extends RuntimeException {
        public WorkflowExecutionException(String message) {
            super(message);
        }

        public WorkflowExecutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

