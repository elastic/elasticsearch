/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Clears the cache in {@link InferenceEndpointRegistry}.
 * This uses the cluster state to broadcast the message to all nodes to clear their cache, which has guaranteed delivery.
 * There are some edge cases where deletes can come from any node, for example ElasticInferenceServiceAuthorizationHandler and
 * SemanticTextIndexOptionsIT will delete endpoints on whatever node is handling the request. So this must use a master node transport
 * action so that the cluster updates can invalidate the cache, even though most requests will originate from the master node
 * (e.g. when updating and deleting inference endpoints via REST).
 */
public class ClearInferenceEndpointCacheAction extends AcknowledgedTransportMasterNodeAction<ClearInferenceEndpointCacheAction.Request> {
    private static final Logger log = LogManager.getLogger(ClearInferenceEndpointCacheAction.class);
    private static final String NAME = "cluster:internal/xpack/inference/clear_inference_endpoint_cache";
    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>(NAME);
    private static final String TASK_QUEUE_NAME = "inference-endpoint-cache-management";

    private static final TransportVersion ML_INFERENCE_ENDPOINT_CACHE = TransportVersion.fromName("ml_inference_endpoint_cache");

    private final ProjectResolver projectResolver;
    private final InferenceEndpointRegistry inferenceEndpointRegistry;
    private final MasterServiceTaskQueue<RefreshCacheMetadataVersionTask> taskQueue;

    @Inject
    public ClearInferenceEndpointCacheAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        InferenceEndpointRegistry inferenceEndpointRegistry
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClearInferenceEndpointCacheAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.inferenceEndpointRegistry = inferenceEndpointRegistry;
        this.taskQueue = clusterService.createTaskQueue(TASK_QUEUE_NAME, Priority.NORMAL, new CacheMetadataUpdateTaskExecutor());
        clusterService.addListener(
            event -> event.state()
                .metadata()
                .projects()
                .values()
                .stream()
                .map(ProjectMetadata::id)
                .filter(id -> event.customMetadataChanged(id, InvalidateCacheMetadata.NAME))
                .peek(id -> log.trace("Inference endpoint cache on node [{}]", () -> event.state().nodes().getLocalNodeId()))
                .forEach(inferenceEndpointRegistry::invalidateAll)
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        if (inferenceEndpointRegistry.cacheEnabled() == false) {
            ActionListener.completeWith(listener, () -> AcknowledgedResponse.TRUE);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        ClearInferenceEndpointCacheAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (inferenceEndpointRegistry.cacheEnabled()) {
            taskQueue.submitTask("invalidateAll", new RefreshCacheMetadataVersionTask(projectResolver.getProjectId(), listener), null);
        } else {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ClearInferenceEndpointCacheAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    public static class Request extends AcknowledgedRequest<ClearInferenceEndpointCacheAction.Request> {
        protected Request() {
            super(INFINITE_MASTER_NODE_TIMEOUT, TimeValue.ZERO);
        }

        protected Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(ackTimeout());
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) return true;
            return other instanceof ClearInferenceEndpointCacheAction.Request that && Objects.equals(that.ackTimeout(), ackTimeout());
        }
    }

    public static class InvalidateCacheMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        public static final String NAME = "inference-endpoint-cache-metadata";
        private static final InvalidateCacheMetadata EMPTY = new InvalidateCacheMetadata(0L);
        private static final ParseField VERSION_FIELD = new ParseField("version");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<InvalidateCacheMetadata, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            true,
            args -> new InvalidateCacheMetadata((long) args[0])
        );

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        }

        public static InvalidateCacheMetadata fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static InvalidateCacheMetadata fromMetadata(ProjectMetadata projectMetadata) {
            InvalidateCacheMetadata metadata = projectMetadata.custom(NAME);
            return metadata == null ? EMPTY : metadata;
        }

        private final long version;

        private InvalidateCacheMetadata(long version) {
            this.version = version;
        }

        public InvalidateCacheMetadata(StreamInput in) throws IOException {
            this(in.readVLong());
        }

        public InvalidateCacheMetadata bumpVersion() {
            return new InvalidateCacheMetadata(version < Long.MAX_VALUE ? version + 1 : 1L);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return ML_INFERENCE_ENDPOINT_CACHE;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(version);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
            return Iterators.single(((builder, params) -> builder.field(VERSION_FIELD.getPreferredName(), version)));
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(version);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) return true;
            return other instanceof InvalidateCacheMetadata that && that.version == this.version;
        }
    }

    private static class RefreshCacheMetadataVersionTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;

        private RefreshCacheMetadataVersionTask(ProjectId projectId, ActionListener<AcknowledgedResponse> listener) {
            super(TimeValue.THIRTY_SECONDS, listener);
            this.projectId = projectId;
        }
    }

    private static class CacheMetadataUpdateTaskExecutor extends SimpleBatchedAckListenerTaskExecutor<RefreshCacheMetadataVersionTask> {
        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(RefreshCacheMetadataVersionTask task, ClusterState clusterState) {
            var projectMetadata = clusterState.metadata().getProject(task.projectId);
            var currentMetadata = InvalidateCacheMetadata.fromMetadata(projectMetadata);
            var updatedMetadata = currentMetadata.bumpVersion();
            var newProjectMetadata = ProjectMetadata.builder(projectMetadata).putCustom(InvalidateCacheMetadata.NAME, updatedMetadata);
            return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(newProjectMetadata).build(), task);
        }
    }
}
