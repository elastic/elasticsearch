/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

public final class StepResponse {

    private final boolean isTaskComplete;

    public StepResponse(boolean isTaskComplete) {
        this.isTaskComplete = isTaskComplete;
    }

    /**
     * Returns whether the entire task has completed after this step was done
     * @return whether the entire task has completed after this step was done
     */
    public boolean isTaskComplete() {
        return isTaskComplete;
    }
}
