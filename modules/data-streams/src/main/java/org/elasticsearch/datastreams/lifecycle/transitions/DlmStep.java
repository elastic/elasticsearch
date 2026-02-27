/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.index.Index;

import java.util.List;

/**
 * A step within a Data Lifecycle Management action. Each step is responsible for determining if it has been completed for a given index
 * and executing the necessary operations to complete the step.
 */
public interface DlmStep {

    /**
     * Determines if the step has been completed for the given index and project state.
     *
     * @param index The index to check.
     * @param projectState The current project state.
     * @return
     */
    boolean stepCompleted(Index index, ProjectState projectState);

    /**
     * This method determines how to execute the step and performs the necessary operations to update the index
     * so that {@link #stepCompleted(Index, ProjectState)} will return true after successful execution.
     *
     * @param dlmStepContext The context and resources for executing the step.
     */
    void execute(DlmStepContext dlmStepContext);

    /**
     * A human-readable name for the step.
     *
     * @return The step name.
     */
    String stepName();

    /**
     * A list of functions that can be used to generate possible output index name patterns for this step based on the input index name.
     * This is used when a step might change the index that is targeted for later steps in the action such as cloning.
     * <br>
     * The list is ordered and the first pattern that generates an index name that exists in the cluster will be used by later steps.
     * <br>
     * If not overridden, this method simply returns a list with the input index name, meaning that by default steps will target
     * the same index as the input index.
     * @param indexName The input index name to generate patterns for
     * @return A list of possible output index name patterns for the given input index name.
     */
    default List<String> possibleOutputIndexNamePatterns(String indexName) {
        return List.of(indexName);
    }
}
