/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Transport action for cleaning up feature index state.
 */
public class TransportResetFeatureStateAction extends HandledTransportAction<ResetFeatureStateRequest, ResetFeatureStateResponse> {

    private final SystemIndices systemIndices;
    private final NodeClient client;
    private final ClusterService clusterService;

    @Inject
    public TransportResetFeatureStateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SystemIndices systemIndices,
        NodeClient client,
        ClusterService clusterService
    ) {
        super(ResetFeatureStateAction.NAME, transportService, actionFilters, ResetFeatureStateRequest::new);
        this.systemIndices = systemIndices;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ResetFeatureStateRequest request, ActionListener<ResetFeatureStateResponse> listener) {

        if (systemIndices.getFeatures().size() == 0) {
            listener.onResponse(new ResetFeatureStateResponse(Collections.emptyList()));
        }

        final int features = systemIndices.getFeatures().size();
        GroupedActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> groupedActionListener = new GroupedActionListener<>(
            listener.map(responses -> {
                assert features == responses.size();
                return new ResetFeatureStateResponse(new ArrayList<>(responses));
            }),
            systemIndices.getFeatures().size()
        );

        for (SystemIndices.Feature feature : systemIndices.getFeatures().values()) {
            feature.getCleanUpFunction().apply(clusterService, client, groupedActionListener);
        }
    }
}
