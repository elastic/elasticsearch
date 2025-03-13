/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Transport action for cleaning up feature index state.
 */
public class TransportResetFeatureStateAction extends TransportMasterNodeAction<ResetFeatureStateRequest, ResetFeatureStateResponse> {

    private final SystemIndices systemIndices;
    private final NodeClient client;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportResetFeatureStateAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SystemIndices systemIndices,
        NodeClient client,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            ResetFeatureStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ResetFeatureStateRequest::fromStream,
            ResetFeatureStateResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndices = systemIndices;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        ResetFeatureStateRequest request,
        ClusterState state,
        ActionListener<ResetFeatureStateResponse> listener
    ) throws Exception {
        final var features = systemIndices.getFeatures();
        final var responses = new ArrayList<ResetFeatureStateResponse.ResetFeatureStateStatus>(features.size());
        try (
            var listeners = new RefCountingListener(
                listener.map(ignored -> new ResetFeatureStateResponse(Collections.unmodifiableList(responses)))
            )
        ) {
            for (final var feature : features) {
                feature.getCleanUpFunction().apply(clusterService, projectResolver, client, listeners.acquire(e -> {
                    assert e != null : feature.getName();
                    synchronized (responses) {
                        responses.add(e);
                    }
                }));
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ResetFeatureStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
