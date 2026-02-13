/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.ErrorRecordingActionListener;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportRequest;

import java.util.function.BiConsumer;

/**
 * Context and resources required for executing a DLM step.
 */
public record DlmStepContext(
    Index index,
    ProjectState projectState,
    ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> transportActionsDeduplicator,
    DataStreamLifecycleErrorStore errorStore,
    int signallingErrorRetryThreshold,
    Client client
) {

    /**
     * Creates a step context from a {@link DlmActionContext} and an index.
     *
     * @param index The index this step context is for.
     * @param actionContext The action context to derive common resources from.
     */
    public DlmStepContext(Index index, DlmActionContext actionContext) {
        this(
            index,
            actionContext.projectState(),
            actionContext.transportActionsDeduplicator(),
            actionContext.errorStore(),
            actionContext.signallingErrorRetryThreshold(),
            actionContext.client()
        );
    }

    /**
     * @return The name of the index associated with this context.
     */
    public String indexName() {
        return index.getName();
    }

    /**
     * @return The project ID associated with this context.
     */
    public ProjectId projectId() {
        return projectState.projectId();
    }

    public void executeDeduplicatedRequest(
        String actionName,
        TransportRequest request,
        String failureMessage,
        BiConsumer<Tuple<ProjectId, TransportRequest>, ActionListener<Void>> callback
    ) {
        transportActionsDeduplicator.executeOnce(
            Tuple.tuple(projectId(), request),
            new ErrorRecordingActionListener(
                actionName,
                projectId(),
                indexName(),
                errorStore,
                failureMessage,
                signallingErrorRetryThreshold
            ),
            callback
        );
    }

    /*
     * @return true if the request is in-progress (deduplicator is currently
     * tracking the provided projectId, request tuple),
     * false otherwise.
     */
    public boolean isRequestInProgress(TransportRequest request) {
        return transportActionsDeduplicator.hasRequest(Tuple.tuple(projectId(), request));
    }
}
