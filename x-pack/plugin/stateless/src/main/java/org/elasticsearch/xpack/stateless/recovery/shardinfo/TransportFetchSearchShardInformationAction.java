/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Transport Action to share the state of the last acquired index searcher across shards.
 *
 * In order to figure out if data should be prefetched when a shard is moving in the cluster,
 * this action allows to ask other nodes for the last updated timestamp of a ShardId.
 *
 * A Request to a node contains just a shardId and the node to be sent to
 *
 * A NodeResponse contains the time of the last acquired searcher in milliseconds since the epoch.
 *
 * The transport action contains the node operation, which queries the SearchEngine instance.
 */
public class TransportFetchSearchShardInformationAction extends HandledTransportAction<
    TransportFetchSearchShardInformationAction.Request,
    TransportFetchSearchShardInformationAction.Response> {

    public static final ActionType<TransportFetchSearchShardInformationAction.Response> TYPE = new ActionType<>(
        "internal:admin/stateless/indices/shard_information"
    );

    private static final Logger logger = LogManager.getLogger(TransportFetchSearchShardInformationAction.class);
    static final Response NO_OTHER_SHARDS_FOUND_RESPONSE = new Response(-1);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final String shardActionName;
    private final AtomicInteger seed = new AtomicInteger(0);
    private final LongSupplier nowSupplier;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportFetchSearchShardInformationAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(TYPE.name(), transportService, actionFilters, TransportFetchSearchShardInformationAction.Request::new, threadPool.generic());
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.shardActionName = actionName + "[s]";
        this.nowSupplier = threadPool::absoluteTimeInMillis;
        transportService.registerRequestHandler(
            shardActionName,
            threadPool.generic(),
            TransportFetchSearchShardInformationAction.Request::new,
            (request, channel, task) -> shardOperation(request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ProjectState projectState = projectResolver.getProjectState(clusterService.state());

        // specific data stream handling
        // if this is the latest write index of a data stream, shortcut and not even send the request to another node
        // Our basic assumption here is, that latest datastreams backing indices are always eligible
        // for caching with the prefetcher, no need to query around in the cluster
        Optional<ProjectMetadata> projectMetadataOptional = clusterService.state()
            .metadata()
            .lookupProject(request.getShardId().getIndex());
        if (projectMetadataOptional.isPresent()) {
            ProjectMetadata projectMetadata = projectMetadataOptional.get();
            String indexName = request.getShardId().getIndex().getName();

            // ensure old indices that are not written anymore into are accidentally considered new
            long now = nowSupplier.getAsLong();
            long indexAgeMilliSeconds = now - projectMetadata.index(indexName).getCreationDate();
            boolean isIndexAgeWithinRange = TimeValue.timeValueDays(30).millis() > indexAgeMilliSeconds;
            IndexAbstraction index = projectMetadata.getIndicesLookup().get(indexName);
            boolean isWriteIndex = index.getParentDataStream() != null
                && indexName.equals(index.getParentDataStream().getWriteIndex().getName());
            if (isIndexAgeWithinRange && isWriteIndex) {
                listener.onResponse(new Response(now));
                return;
            }
        }

        Optional<ShardRouting> searchShard = findSearchShard(projectState, request);
        if (searchShard.isEmpty()) {
            listener.onResponse(NO_OTHER_SHARDS_FOUND_RESPONSE);
            return;
        }

        ShardRouting shardRouting = searchShard.get();
        DiscoveryNode node = clusterService.state().nodes().resolveNode(shardRouting.currentNodeId());

        logger.trace("requesting shard information from shard {}", shardRouting);
        transportService.sendChildRequest(
            node,
            shardActionName,
            request,
            task,
            TransportRequestOptions.timeout(TimeValue.THIRTY_SECONDS),
            new ActionListenerResponseHandler<>(
                listener,
                TransportFetchSearchShardInformationAction.Response::new,
                TransportResponseHandler.TRANSPORT_WORKER
            )
        );
    }

    // visible for testing
    void shardOperation(Request request, ActionListener<Response> listener) {
        ActionListener.completeWith(listener, () -> {
            IndexShard indexShard = indicesService.getShardOrNull(request.getShardId());
            if (indexShard == null) {
                throw new ShardNotFoundException(request.getShardId());
            }

            long lastSearcherAcquired = indexShard.tryWithEngineOrNull(engine -> {
                if (engine instanceof SearchEngine searchEngine) {
                    return searchEngine.getLastSearcherAcquiredTime();
                }

                return 0L;
            });

            return new Response(lastSearcherAcquired);
        });
    }

    // visible for testing
    Optional<ShardRouting> findSearchShard(ProjectState projectState, Request request) {
        ClusterState state = projectState.cluster();
        RoutingTable routingTable = state.routingTable(projectState.projectId());

        List<ShardRouting> activeShardRouting = routingTable.shardRoutingTable(request.getShardId()).activeShards();
        String localNodeId = state.nodes().getLocalNodeId();

        List<ShardRouting> shardRoutings = new ArrayList<>();
        for (ShardRouting shardRouting : activeShardRouting) {
            // find other nodes, this shard resides on, ensure they are active, on other search only nodes and not this node
            if (shardRouting.isSearchable() && localNodeId.equals(shardRouting.currentNodeId()) == false) {
                // if we found the requested node, break out of the loop and use only that
                if (shardRouting.currentNodeId().equals(request.getNodeId())) {
                    shardRoutings = List.of(shardRouting);
                    break;
                } else {
                    shardRoutings.add(shardRouting);
                }
            }
        }

        if (shardRoutings.isEmpty()) {
            logger.trace("found no copies for shard {} to request shard state information", request.getShardId());
            return Optional.empty();
        } else {
            // most simple randomizing
            if (shardRoutings.size() > 1) {
                Collections.rotate(shardRoutings, seed.incrementAndGet());
            }

            ShardRouting candidate = shardRoutings.get(0);
            return Optional.of(candidate);
        }
    }

    public static class Request extends ActionRequest {

        private final String nodeId;
        private final ShardId shardId;

        public Request(@Nullable String nodeId, ShardId shardId) {
            this.shardId = shardId;
            this.nodeId = nodeId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            nodeId = in.readOptionalString();
        }

        public String getNodeId() {
            return nodeId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeOptionalString(nodeId);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId) && Objects.equals(shardId, request.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, shardId);
        }
    }

    public static final class Response extends ActionResponse {

        private final long lastSearcherAcquiredTime;

        public Response(long lastSearcherAcquiredTime) {
            this.lastSearcherAcquiredTime = lastSearcherAcquiredTime;
        }

        public Response(StreamInput in) throws IOException {
            lastSearcherAcquiredTime = in.readVLong();
        }

        public long getLastSearcherAcquiredTime() {
            return lastSearcherAcquiredTime;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(lastSearcherAcquiredTime);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(lastSearcherAcquiredTime, response.lastSearcherAcquiredTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lastSearcherAcquiredTime);
        }
    }
}
