/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Sends the refresh of the unpromotable copies, with a version of the table as the generation, to the search replicas of a shard of the
 * write engine: each replica reads the table until it shows that version (see {@code ParquetTableFollowEngine}). It is the request that
 * {@code _refresh} and {@code refresh=wait_for} send after the primary refreshed, sent here when the primary committed a new version.
 *
 * <p>A plugin cannot send the unpromotable refresh itself (it has no transport service), so this local action does it. Its name is below
 * {@code indices:admin/refresh}, which the internal user of the {@code post_write_refresh} origin may run on any index, as it may run the
 * unpromotable refresh.
 */
public final class PublishTableVersionAction {

    public static final String NAME = RefreshAction.NAME + "/parquet_table";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);
    /** How long the sender waits for the replicas to show the version. */
    static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(5);

    private PublishTableVersionAction() {}

    /** The shard, and the version of its table that the primary committed in that primary term. */
    public static final class Request extends ActionRequest implements IndicesRequest {

        private final ShardId shardId;
        private final long primaryTerm;
        private final long version;

        public Request(ShardId shardId, long primaryTerm, long version) {
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.version = version;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.primaryTerm = in.readVLong();
            this.version = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeVLong(primaryTerm);
            out.writeVLong(version);
        }

        public ShardId shardId() {
            return shardId;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return version;
        }

        @Override
        public String[] indices() {
            return new String[] { shardId.getIndexName() };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "publish version [" + version + "] of the table of " + shardId + " in primary term [" + primaryTerm + "]";
        }
    }

    public static final class TransportAction extends HandledTransportAction<Request, ActionResponse.Empty> {

        private final TransportService transportService;
        private final ClusterService clusterService;
        private final ThreadPool threadPool;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            ThreadPool threadPool
        ) {
            super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.transportService = transportService;
            this.clusterService = clusterService;
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
            final ClusterState state = clusterService.state();
            final IndexShardRoutingTable routing;
            try {
                routing = state.routingTable(state.metadata().projectFor(request.shardId().getIndex()).id())
                    .shardRoutingTable(request.shardId());
            } catch (IndexNotFoundException | ShardNotFoundException e) {
                // the index was deleted meanwhile: there is no replica to tell
                listener.onResponse(ActionResponse.Empty.INSTANCE);
                return;
            }
            if (routing.unpromotableShards().isEmpty()) {
                listener.onResponse(ActionResponse.Empty.INSTANCE);
                return;
            }
            // failShardOnError is false: a replica that cannot read the table now keeps its files, and reads it on its next refresh
            final UnpromotableShardRefreshRequest refresh = new UnpromotableShardRefreshRequest(
                routing,
                request.primaryTerm(),
                request.version(),
                false
            );
            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportUnpromotableShardRefreshAction.NAME,
                refresh,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(
                    // a replica that cannot read the table keeps the request until it can, and the caller only logs the outcome
                    ActionListener.addTimeout(TIMEOUT, threadPool, threadPool.executor(ThreadPool.Names.REFRESH), listener),
                    in -> ActionResponse.Empty.INSTANCE,
                    threadPool.executor(ThreadPool.Names.REFRESH)
                )
            );
        }
    }
}
