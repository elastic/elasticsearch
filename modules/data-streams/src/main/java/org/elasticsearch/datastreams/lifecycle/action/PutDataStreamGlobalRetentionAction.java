/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Sets the global retention for data streams (if it's not a dry run) and it returns the affected data streams.
 */
public class PutDataStreamGlobalRetentionAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:admin/data_stream/global_retention/put");

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
            return null;
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

    public static final class Response extends ActionResponse implements ChunkedToXContentObject {

        private final boolean acknowledged;
        private final boolean dryRun;
        private final List<AffectedDataStream> affectedDataStreams;

        public Response(StreamInput in) throws IOException {
            super(in);
            acknowledged = in.readBoolean();
            dryRun = in.readBoolean();
            affectedDataStreams = in.readCollectionAsImmutableList(AffectedDataStream::read);
        }

        public Response(boolean acknowledged, boolean dryRun) {
            this.acknowledged = acknowledged;
            this.dryRun = dryRun;
            this.affectedDataStreams = List.of();
        }

        public Response(boolean acknowledged, boolean dryRun, List<AffectedDataStream> affectedDataStreams) {
            this.acknowledged = acknowledged;
            this.dryRun = dryRun;
            this.affectedDataStreams = affectedDataStreams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(acknowledged);
            out.writeBoolean(dryRun);
            out.writeCollection(affectedDataStreams);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(ChunkedToXContentHelper.startObject(), Iterators.single(((builder, params1) -> {
                builder.field("acknowledged", acknowledged);
                builder.field("dry_run", dryRun);
                return builder;
            })),
                ChunkedToXContentHelper.startArray("affected_data_streams"),
                Iterators.map(affectedDataStreams.iterator(), affectedDataStream -> affectedDataStream::toXContent),
                ChunkedToXContentHelper.endArray(),
                ChunkedToXContentHelper.endObject()
            );
        }

        public record AffectedDataStream(String dataStreamName, TimeValue newEffectiveRetention, TimeValue previousEffectiveRetention)
            implements
                Writeable,
                ToXContentObject {

            public static AffectedDataStream read(StreamInput in) throws IOException {
                return new AffectedDataStream(in.readString(), in.readOptionalTimeValue(), in.readOptionalTimeValue());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(dataStreamName);
                out.writeOptionalTimeValue(newEffectiveRetention);
                out.writeOptionalTimeValue(previousEffectiveRetention);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("name", dataStreamName);
                builder.field("new_effective_retention", newEffectiveRetention == null ? "infinite" : newEffectiveRetention.getStringRep());
                builder.field(
                    "previous_effective_retention",
                    previousEffectiveRetention == null ? "infinite" : previousEffectiveRetention.getStringRep()
                );
                builder.endObject();
                return builder;
            }
        }
    }

    public static class TransportPutDataStreamGlobalRetentionAction extends TransportMasterNodeAction<Request, Response> {

        private final FeatureService featureService;
        private final ClusterStateTaskExecutor<UpsertGlobalDataStreamMetadataTask> executor;
        private final MasterServiceTaskQueue<UpsertGlobalDataStreamMetadataTask> taskQueue;

        @Inject
        public TransportPutDataStreamGlobalRetentionAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
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
                Response::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.featureService = featureService;
            this.executor = new SimpleBatchedAckListenerTaskExecutor<>() {

                @Override
                public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                    UpsertGlobalDataStreamMetadataTask task,
                    ClusterState clusterState
                ) {
                    return new Tuple<>(updateGlobalRetention(clusterState, task.request.getGlobalRetention()), task);
                }
            };
            this.taskQueue = clusterService.createTaskQueue("data-stream-global-settings", Priority.HIGH, this.executor);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            if (featureService.clusterHasFeature(state, DataStreamGlobalRetention.DATA_STREAM_GLOBAL_RETENTION) == false) {
                listener.onResponse(new Response(false, request.dryRun()));
            }
            updateGlobalRetention(request, state, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        void updateGlobalRetention(
            PutDataStreamGlobalRetentionAction.Request request,
            ClusterState clusterState,
            final ActionListener<PutDataStreamGlobalRetentionAction.Response> listener
        ) {
            List<PutDataStreamGlobalRetentionAction.Response.AffectedDataStream> affectedDataStreams = determineAffectedDataStreams(
                request.getGlobalRetention(),
                clusterState
            );
            if (request.dryRun()) {
                listener.onResponse(new PutDataStreamGlobalRetentionAction.Response(false, true, affectedDataStreams));
            } else {
                taskQueue.submitTask(
                    "update-global-data-stream-settings",
                    new UpsertGlobalDataStreamMetadataTask(request, affectedDataStreams, listener),
                    request.masterNodeTimeout()
                );
            }
        }

        List<PutDataStreamGlobalRetentionAction.Response.AffectedDataStream> determineAffectedDataStreams(
            DataStreamGlobalRetention newGlobalRetention,
            ClusterState clusterState
        ) {
            var previousGlobalRetention = DataStreamGlobalRetention.getFromClusterState(clusterState);
            if (newGlobalRetention.equals(previousGlobalRetention)) {
                return List.of();
            }
            List<PutDataStreamGlobalRetentionAction.Response.AffectedDataStream> affectedDataStreams = new ArrayList<>();
            for (DataStream dataStream : clusterState.metadata().dataStreams().values()) {
                if (dataStream.getLifecycle() != null) {
                    TimeValue previousEffectiveRetention = dataStream.getLifecycle().getEffectiveDataRetention(previousGlobalRetention);
                    TimeValue newEffectiveRetention = dataStream.getLifecycle().getEffectiveDataRetention(newGlobalRetention);
                    if (Objects.equals(previousEffectiveRetention, newEffectiveRetention) == false) {
                        affectedDataStreams.add(
                            new PutDataStreamGlobalRetentionAction.Response.AffectedDataStream(
                                dataStream.getName(),
                                newEffectiveRetention,
                                previousEffectiveRetention
                            )
                        );
                    }
                }
            }
            affectedDataStreams.sort(Comparator.comparing(Response.AffectedDataStream::dataStreamName));
            return affectedDataStreams;
        }

        private ClusterState updateGlobalRetention(ClusterState clusterState, DataStreamGlobalRetention newRetention) {
            final var initialRetention = DataStreamGlobalRetention.getFromClusterState(clusterState);
            return newRetention.equals(initialRetention)
                ? clusterState
                : clusterState.copyAndUpdate(b -> b.putCustom(DataStreamGlobalRetention.TYPE, newRetention));
        }

        /**
         * A base class for health metadata cluster state update tasks.
         */
        record UpsertGlobalDataStreamMetadataTask(
            PutDataStreamGlobalRetentionAction.Request request,
            List<PutDataStreamGlobalRetentionAction.Response.AffectedDataStream> affectedDataStreams,
            ActionListener<PutDataStreamGlobalRetentionAction.Response> listener
        ) implements ClusterStateTaskListener, ClusterStateAckListener {

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked() {
                listener.onResponse(new PutDataStreamGlobalRetentionAction.Response(true, false, affectedDataStreams));
            }

            @Override
            public void onAckFailure(Exception e) {
                listener.onResponse(new PutDataStreamGlobalRetentionAction.Response(false, request.dryRun()));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new PutDataStreamGlobalRetentionAction.Response(false, request.dryRun()));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.masterNodeTimeout();
            }
        }
    }
}
