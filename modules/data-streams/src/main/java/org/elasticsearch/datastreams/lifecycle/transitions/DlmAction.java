/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.function.Function;

/**
 * An action within Data Lifecycle Management that consists of multiple steps to be executed in sequence.
 * It represents a transition from one lifecycle phase to another, such as moving from hot to frozen storage.
 */
public interface DlmAction {

    /**
     * A human-readable name for the action.
     *
     * @return The action name.
     */
    String name();

    /**
     * A function that extracts the scheduling property ({@link TimeValue}) from the datastream's {@link DataStreamLifecycle}
     * configuration.
     * This ({@link TimeValue}) determines how old an index should be for this action to be triggered and the index transitioned
     *
     * @return A function that takes a DataStreamLifecycle and returns the scheduling TimeValue.
     */
    Function<DataStreamLifecycle, TimeValue> applyAfterTime();

    /**
     * The ordered list of steps that make up this action that must be executed sequentially to complete the action.
     *
     * @return A list of DlmStep instances representing the steps of the action.
     */
    List<DlmStep> steps();

    /**
     * Indicates whether this action applies to the failure store.
     * By default, actions do not apply to the failure store.
     *
     * @return true if the action applies to the failure store, false otherwise.
     */
    default boolean appliesToFailureStore() {
        return false;
    }

    /**
     * Determines whether this action can run on the given project. This allows actions to be skipped entirely
     * if the cluster is not in a compatible state, for example if a required default snapshot repository is not
     * configured or available.
     *
     * @param dlmActionContext The context providing access to the project state and resources.
     * @return true if the action can proceed, false if it should be skipped for this project.
     */
    boolean canRunOnProject(DlmActionContext dlmActionContext);
}
