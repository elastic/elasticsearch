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
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
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
 * API that lazily rolls over a data stream that has the flag {@link DataStream#rolloverOnWrite()} enabled. These requests always
 * originate from requests that write into the data stream.
 */
public final class LazyRolloverAction extends ActionType<RolloverResponse> {

    public static final NodeFeature DATA_STREAM_LAZY_ROLLOVER = new NodeFeature("data_stream.rollover.lazy");

    public static final LazyRolloverAction INSTANCE = new LazyRolloverAction();
    public static final String NAME = "indices:admin/data_stream/lazy_rollover";

    private LazyRolloverAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static final class TransportLazyRolloverAction extends TransportRolloverAction {

        @Inject
        public TransportLazyRolloverAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataRolloverService rolloverService,
            AllocationService allocationService,
            MetadataDataStreamsService metadataDataStreamsService,
            DataStreamAutoShardingService dataStreamAutoShardingService,
            Client client
        ) {
            super(
                LazyRolloverAction.INSTANCE,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indexNameExpressionResolver,
                rolloverService,
                client,
                allocationService,
                metadataDataStreamsService,
                dataStreamAutoShardingService
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

            assert rolloverRequest.getConditions().hasConditions() == false
                && rolloverRequest.isDryRun() == false
                && rolloverRequest.isLazy() == false
                : "The auto rollover action does not expect any other parameters in the request apart from the data stream name";

            Metadata metadata = clusterState.metadata();
            // We evaluate the names of the source index as well as what our newly created index would be.
            final MetadataRolloverService.NameResolution trialRolloverNames = MetadataRolloverService.resolveRolloverNames(
                clusterState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest(),
                false
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

            String source = "lazy_rollover source [" + trialRolloverIndexName + "] to target [" + trialRolloverIndexName + "]";
            // We create a new rollover request to ensure that it doesn't contain any other parameters apart from the data stream name
            // This will provide a more resilient user experience
            RolloverTask rolloverTask = new RolloverTask(
                new RolloverRequest(rolloverRequest.getRolloverTarget(), null),
                null,
                trialRolloverResponse,
                null,
                listener
            );
            submitRolloverTask(rolloverRequest, source, rolloverTask);
        }
    }
}
