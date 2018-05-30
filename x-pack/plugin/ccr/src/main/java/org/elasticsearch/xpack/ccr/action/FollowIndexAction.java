/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class FollowIndexAction extends Action<FollowIndexAction.Request,
        FollowIndexAction.Response, FollowIndexAction.RequestBuilder> {

    public static final FollowIndexAction INSTANCE = new FollowIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/follow_index";

    private FollowIndexAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        private String leaderIndex;
        private String followIndex;
        private long batchSize = ShardFollowTasksExecutor.DEFAULT_BATCH_SIZE;
        private int concurrentProcessors = ShardFollowTasksExecutor.DEFAULT_CONCURRENT_PROCESSORS;
        private long processorMaxTranslogBytes = ShardFollowTasksExecutor.DEFAULT_MAX_TRANSLOG_BYTES;

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public void setLeaderIndex(String leaderIndex) {
            this.leaderIndex = leaderIndex;
        }

        public String getFollowIndex() {
            return followIndex;
        }

        public void setFollowIndex(String followIndex) {
            this.followIndex = followIndex;
        }

        public long getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(long batchSize) {
            if (batchSize < 1) {
                throw new IllegalArgumentException("Illegal batch_size [" + batchSize + "]");
            }

            this.batchSize = batchSize;
        }

        public void setConcurrentProcessors(int concurrentProcessors) {
            if (concurrentProcessors < 1) {
                throw new IllegalArgumentException("concurrent_processors must be larger than 0");
            }
            this.concurrentProcessors = concurrentProcessors;
        }

        public void setProcessorMaxTranslogBytes(long processorMaxTranslogBytes) {
            if (processorMaxTranslogBytes <= 0) {
                throw new IllegalArgumentException("processor_max_translog_bytes must be larger than 0");
            }
            this.processorMaxTranslogBytes = processorMaxTranslogBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            leaderIndex = in.readString();
            followIndex = in.readString();
            batchSize = in.readVLong();
            processorMaxTranslogBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderIndex);
            out.writeString(followIndex);
            out.writeVLong(batchSize);
            out.writeVLong(processorMaxTranslogBytes);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, FollowIndexAction.RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        Response() {
        }

        Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, Client client, ClusterService clusterService,
                               PersistentTasksService persistentTasksService) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            ClusterState localClusterState = clusterService.state();
            IndexMetaData followIndexMetadata = localClusterState.getMetaData().index(request.followIndex);

            String[] indices = new String[]{request.getLeaderIndex()};
            Map<String, List<String>> remoteClusterIndices = remoteClusterService.groupClusterIndices(indices, s -> false);
            if (remoteClusterIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                // Following an index in local cluster, so use local cluster state to fetch leader IndexMetaData:
                IndexMetaData leaderIndexMetadata = localClusterState.getMetaData().index(request.leaderIndex);
                start(request, null, leaderIndexMetadata, followIndexMetadata, listener);
            } else {
                // Following an index in remote cluster, so use remote client to fetch leader IndexMetaData:
                assert remoteClusterIndices.size() == 1;
                Map.Entry<String, List<String>> entry = remoteClusterIndices.entrySet().iterator().next();
                assert entry.getValue().size() == 1;
                String clusterNameAlias = entry.getKey();
                String leaderIndex = entry.getValue().get(0);

                Client remoteClient = client.getRemoteClusterClient(clusterNameAlias);
                ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                clusterStateRequest.clear();
                clusterStateRequest.metaData(true);
                clusterStateRequest.indices(leaderIndex);
                remoteClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(r -> {
                    ClusterState remoteClusterState = r.getState();
                    IndexMetaData leaderIndexMetadata = remoteClusterState.getMetaData().index(leaderIndex);
                    start(request, clusterNameAlias, leaderIndexMetadata, followIndexMetadata, listener);
                }, listener::onFailure));
            }
        }

        /**
         * Performs validation on the provided leader and follow {@link IndexMetaData} instances and then
         * creates a persistent task for each leader primary shard. This persistent tasks track changes in the leader
         * shard and replicate these changes to a follower shard.
         *
         * Currently the following validation is performed:
         * <ul>
         *     <li>The leader index and follow index need to have the same number of primary shards</li>
         * </ul>
         */
        void start(Request request, String clusterNameAlias, IndexMetaData leaderIndexMetadata, IndexMetaData followIndexMetadata,
                   ActionListener<Response> handler) {
            validate (leaderIndexMetadata ,followIndexMetadata , request);
                final int numShards = followIndexMetadata.getNumberOfShards();
                final AtomicInteger counter = new AtomicInteger(numShards);
                final AtomicReferenceArray<Object> responses = new AtomicReferenceArray<>(followIndexMetadata.getNumberOfShards());
                Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                    .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));for (int i = 0; i < numShards; i++) {
                    final int shardId = i;
                    String taskId = followIndexMetadata.getIndexUUID() + "-" + shardId;
                    ShardFollowTask shardFollowTask = new ShardFollowTask(clusterNameAlias,
                            new ShardId(followIndexMetadata.getIndex(), shardId),
                            new ShardId(leaderIndexMetadata.getIndex(), shardId),
                            request.batchSize, request.concurrentProcessors, request.processorMaxTranslogBytes, filteredHeaders);
                    persistentTasksService.sendStartRequest(taskId, ShardFollowTask.NAME, shardFollowTask,
                            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask>>() {
                                @Override
                                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask> task) {
                                    responses.set(shardId, task);
                                    finalizeResponse();
                                }

                            @Override
                            public void onFailure(Exception e) {
                                responses.set(shardId, e);
                                finalizeResponse();
                            }

                            void finalizeResponse() {
                                Exception error = null;
                                if (counter.decrementAndGet() == 0) {
                                    for (int j = 0; j < responses.length(); j++) {
                                        Object response = responses.get(j);
                                        if (response instanceof Exception) {
                                            if (error == null) {
                                                error = (Exception) response;
                                            } else {
                                                error.addSuppressed((Throwable) response);
                                            }
                                        }
                                    }

                                    if (error == null) {
                                        // include task ids?
                                        handler.onResponse(new Response(true));
                                    } else {
                                        // TODO: cancel all started tasks
                                        handler.onFailure(error);
                                    }
                                }
                            }
                        }
                );
            }
        }
    }


    static void validate(IndexMetaData leaderIndex, IndexMetaData followIndex, Request request) {
        if (leaderIndex == null) {
            throw new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not exist");
        }

        if (followIndex == null) {
            throw new IllegalArgumentException("follow index [" + request.followIndex + "] does not exist");
        }
        if (leaderIndex.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false) == false) {
            throw new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not have soft deletes enabled");
        }

        if (leaderIndex.getNumberOfShards() != followIndex.getNumberOfShards()) {
            throw new IllegalArgumentException("leader index primary shards [" + leaderIndex.getNumberOfShards() +
                "] does not match with the number of shards of the follow index [" + followIndex.getNumberOfShards() + "]");
        }
        // TODO: other validation checks
    }

}
