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
import org.elasticsearch.action.ValidateActions;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.UpdateDataStreamGlobalRetentionService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Sets the global retention for data streams (if it's not a dry run) and it returns the affected data streams.
 */
public class PutDataStreamGlobalRetentionAction {

    public static final ActionType<UpdateDataStreamGlobalRetentionResponse> INSTANCE = new ActionType<>(
        "cluster:admin/data_stream/global_retention/put"
    );

    private PutDataStreamGlobalRetentionAction() {/* no instances */}

    public static final class Request extends MasterNodeRequest<Request> {

        public static final ConstructingObjectParser<PutDataStreamGlobalRetentionAction.Request, Void> PARSER =
            new ConstructingObjectParser<>(
                "put_data_stream_global_retention_request",
                args -> new PutDataStreamGlobalRetentionAction.Request((TimeValue) args[0], (TimeValue) args[1])
            );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DataStreamGlobalRetention.DEFAULT_RETENTION_FIELD.getPreferredName()),
                DataStreamGlobalRetention.DEFAULT_RETENTION_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DataStreamGlobalRetention.MAX_RETENTION_FIELD.getPreferredName()),
                DataStreamGlobalRetention.MAX_RETENTION_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
        }

        private final DataStreamGlobalRetention globalRetention;
        private boolean dryRun = false;

        public static PutDataStreamGlobalRetentionAction.Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            globalRetention = DataStreamGlobalRetention.read(in);
            dryRun = in.readBoolean();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (globalRetention.equals(DataStreamGlobalRetention.EMPTY)) {
                return ValidateActions.addValidationError(
                    "At least one of 'default_retention' or 'max_retention' should be defined."
                        + " If you want to remove the configuration please use the DELETE method",
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            globalRetention.writeTo(out);
            out.writeBoolean(dryRun);
        }

        public Request(@Nullable TimeValue defaultRetention, @Nullable TimeValue maxRetention) {
            this.globalRetention = new DataStreamGlobalRetention(defaultRetention, maxRetention);
        }

        public DataStreamGlobalRetention getGlobalRetention() {
            return globalRetention;
        }

        public boolean dryRun() {
            return dryRun;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PutDataStreamGlobalRetentionAction.Request request = (PutDataStreamGlobalRetentionAction.Request) o;
            return Objects.equals(globalRetention, request.globalRetention) && dryRun == request.dryRun;
        }

        @Override
        public int hashCode() {
            return Objects.hash(globalRetention, dryRun);
        }

        public void dryRun(boolean dryRun) {
            this.dryRun = dryRun;
        }
    }

    public static class TransportPutDataStreamGlobalRetentionAction extends TransportMasterNodeAction<
        Request,
        UpdateDataStreamGlobalRetentionResponse> {

        private final UpdateDataStreamGlobalRetentionService globalRetentionService;
        private final FeatureService featureService;

        @Inject
        public TransportPutDataStreamGlobalRetentionAction(
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
                .determineAffectedDataStreams(request.globalRetention, state);
            if (request.dryRun()) {
                listener.onResponse(new UpdateDataStreamGlobalRetentionResponse(false, true, affectedDataStreams));
            } else {
                globalRetentionService.updateGlobalRetention(request, affectedDataStreams, listener);
            }
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
