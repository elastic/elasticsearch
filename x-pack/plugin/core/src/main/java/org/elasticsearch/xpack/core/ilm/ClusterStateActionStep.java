/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;

/**
 * Updates the cluster state, similar to {@link org.elasticsearch.cluster.ClusterStateUpdateTask}.
 */
public abstract class ClusterStateActionStep extends Step {

    public ClusterStateActionStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public abstract ProjectState performAction(Index index, ProjectState projectState);

    /**
     * Returns a tuple of index name to step key for an index *other* than the
     * index ILM is currently processing. This is used when a new index is
     * spawned by ILM and its initial action needs to be to invoked in the event
     * that it is an {@link AsyncActionStep}.
     */
    public Tuple<String, StepKey> indexForAsyncInvocation() {
        return null;
    }
}
