/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.datastreams.lifecycle.UpdateDataStreamGlobalRetentionService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Sets the global retention for data streams (if it's not a dry run) and it returns the affected data streams.
 */
public class DeleteDataStreamGlobalRetentionAction {

    public static final ActionType<UpdateDataStreamGlobalRetentionResponse> INSTANCE = new ActionType<>(
        "cluster:admin/data_stream/global_retention/delete"
    );

    private DeleteDataStreamGlobalRetentionAction() {/* no instances */}

    public static final class Request extends MasterNodeRequest<Request> {
        private boolean dryRun = false;

        public Request(StreamInput in) throws IOException {
            super(in);
            dryRun = in.readBoolean();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(dryRun);
        }

        public Request() {}

        public boolean dryRun() {
            return dryRun;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteDataStreamGlobalRetentionAction.Request request = (DeleteDataStreamGlobalRetentionAction.Request) o;
            return dryRun == request.dryRun;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dryRun);
        }

        public void dryRun(boolean dryRun) {
            this.dryRun = dryRun;
        }
    }

    public static class TransportDeleteDataStreamGlobalRetentionAction extends TransportMasterNodeAction<
        Request,
        UpdateDataStreamGlobalRetentionResponse> {

        private final UpdateDataStreamGlobalRetentionService globalRetentionService;
        private final FeatureService featureService;

        @Inject
        public TransportDeleteDataStreamGlobalRetentionAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            UpdateDataStreamGlobalRetentionService globalRetentionService,
            FeatureService featureService
        ) {
            super(
                INSTANCE.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                UpdateDataStreamGlobalRetentionResponse::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.globalRetentionService = globalRetentionService;
            this.featureService = featureService;
        }

        @Override
        protected void masterOperation(
            Task task,
            Request request,
            ClusterState state,
            ActionListener<UpdateDataStreamGlobalRetentionResponse> listener
        ) throws Exception {
            if (featureService.clusterHasFeature(state, DataStreamGlobalRetention.GLOBAL_RETENTION)) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "Data stream global retention feature not found, please ensure all nodes have the feature "
                            + DataStreamGlobalRetention.GLOBAL_RETENTION.id()
                    )
                );
                return;
            }
            List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams = globalRetentionService
                .determineAffectedDataStreams(null, state);
            if (request.dryRun()) {
                listener.onResponse(new UpdateDataStreamGlobalRetentionResponse(false, true, affectedDataStreams));
            } else {
                globalRetentionService.removeGlobalRetention(request, affectedDataStreams, listener);
            }
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
