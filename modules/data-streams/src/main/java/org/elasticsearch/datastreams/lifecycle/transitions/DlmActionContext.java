/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions;

import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportRequest;

/**
 * Context and resources required for executing a DLM action.
 */
public record DlmActionContext(
    ProjectState projectState,
    ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> transportActionsDeduplicator,
    DataStreamLifecycleErrorStore errorStore,
    int signallingErrorRetryThreshold,
    Client client
) {
    /**
     * @return The project ID associated with this context.
     */
    public ProjectId projectId() {
        return projectState.projectId();
    }

    /**
     * Creates a {@link DlmStepContext} for the given index, using the resources from this action context.
     *
     * @param index The index to create a step context for.
     * @return A new {@link DlmStepContext} for the given index.
     */
    public DlmStepContext stepContextFor(Index index) {
        return new DlmStepContext(index, this);
    }

}
