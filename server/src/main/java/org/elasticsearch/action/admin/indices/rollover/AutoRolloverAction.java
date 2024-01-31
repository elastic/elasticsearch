/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * Api that automatically rolls over a data stream that has the flag {@link DataStream#rolloverOnWrite()} enabled. These requests always
 * originate from requests that write into the data stream.
 */
public final class AutoRolloverAction extends ActionType<RolloverResponse> {

    public static final NodeFeature DATA_STREAM_AUTO_ROLLOVER = new NodeFeature("data_stream.rollover.auto");

    public static final AutoRolloverAction INSTANCE = new AutoRolloverAction();
    public static final String NAME = "indices:admin/data_stream/auto_rollover";

    private AutoRolloverAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static final class TransportAction extends TransportRolloverAction {

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataRolloverService rolloverService,
            AllocationService allocationService,
            MetadataDataStreamsService metadataDataStreamsService,
            Client client
        ) {
            super(
                AutoRolloverAction.INSTANCE,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indexNameExpressionResolver,
                rolloverService,
                client,
                allocationService,
                metadataDataStreamsService
            );
        }

        @Override
        protected void masterOperation(
            Task task,
            RolloverRequest rolloverRequest,
            ClusterState clusterState,
            ActionListener<RolloverResponse> listener
        ) throws Exception {
            assert task instanceof CancellableTask;
            Metadata metadata = clusterState.metadata();
            // We evaluate the names of the source index as well as what our newly created index would be.
            final MetadataRolloverService.NameResolution trialRolloverNames = MetadataRolloverService.resolveRolloverNames(
                clusterState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest()
            );
            final String trialSourceIndexName = trialRolloverNames.sourceName();
            final String trialRolloverIndexName = trialRolloverNames.rolloverName();
            MetadataRolloverService.validateIndexName(clusterState, trialRolloverIndexName);

            assert metadata.dataStreams().containsKey(rolloverRequest.getRolloverTarget()) : "Auto-rollover applies only to data streams";

            final RolloverResponse trialRolloverResponse = new RolloverResponse(
                trialSourceIndexName,
                trialRolloverIndexName,
                Map.of(),
                false,
                false,
                false,
                false,
                false
            );

            String source = "auto_rollover source [" + trialRolloverIndexName + "] to target [" + trialRolloverIndexName + "]";
            RolloverTask rolloverTask = new RolloverTask(rolloverRequest, null, trialRolloverResponse, listener);
            submitRolloverTask(rolloverRequest, source, rolloverTask);
        }
    }
}
