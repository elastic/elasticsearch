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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

public class TransportResetFeatureStateAction extends HandledTransportAction<ResetFeatureStateRequest, ResetFeatureStateResponse> {

    private final SystemIndices systemIndices;
    private final MetadataDeleteIndexService deleteIndexService;
    private final NodeClient client;

    @Inject
    public TransportResetFeatureStateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SystemIndices systemIndices,
        MetadataDeleteIndexService deleteIndexService,
        NodeClient client
    ) {
        super(ResetFeatureStateAction.NAME, transportService, actionFilters,
            ResetFeatureStateRequest::new);
        this.systemIndices = systemIndices;
        this.deleteIndexService = deleteIndexService;
        this.client = client;
    }

    @Override
    protected void doExecute(
        Task task,
        ResetFeatureStateRequest request,
        ActionListener<ResetFeatureStateResponse> listener) {

        for (SystemIndices.Feature feature : systemIndices.getFeatures().values()) {
            feature.getCleanUpFunction().apply(
                Set.of(".test-system-idx", ".associated-idx"),
                client,
                listener);
        }
    }
}
