/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.cluster.ClusterState;

public class StepResult {
    private final String message;
    private final Exception exception;
    private final ClusterState clusterState;
    private final boolean complete;
    private final boolean indexSurvived;

    public StepResult(String message, Exception exception, ClusterState clusterState, boolean indexSurvived, boolean complete) {
        this.message = message;
        this.exception = exception;
        this.clusterState = clusterState;
        this.indexSurvived = indexSurvived;
        this.complete = complete;
    }

    public StepResult(StepResult result, ClusterState newClusterState) {
        this(result.getMessage(), result.getException(), newClusterState, result.indexSurvived(), result.isComplete());
    }

    public String getMessage() {
        return message;
    }

    public Exception getException() {
        return exception;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public boolean indexSurvived() {
        return indexSurvived;
    }

    public boolean isComplete() {
        return complete;
    }
}
