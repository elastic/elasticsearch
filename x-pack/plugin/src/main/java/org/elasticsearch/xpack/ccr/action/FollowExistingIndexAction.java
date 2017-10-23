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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class FollowExistingIndexAction extends Action<FollowExistingIndexAction.Request,
        FollowExistingIndexAction.Response, FollowExistingIndexAction.RequestBuilder> {

    public static final FollowExistingIndexAction INSTANCE = new FollowExistingIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/follow_existing_index";

    private FollowExistingIndexAction() {
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

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            leaderIndex = in.readString();
            followIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderIndex);
            out.writeString(followIndex);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, FollowExistingIndexAction.RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse {

    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final InternalClient client;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, InternalClient client,
                               PersistentTasksService persistentTasksService) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            client.admin().cluster().state(new ClusterStateRequest(), new ActionListener<ClusterStateResponse>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    IndexMetaData leaderIndexMetadata = clusterStateResponse.getState().getMetaData()
                            .index(request.leaderIndex);
                    if (leaderIndexMetadata == null) {
                        listener.onFailure(new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not exist"));
                    }

                    IndexMetaData followIndexMetadata = clusterStateResponse.getState().getMetaData()
                            .index(request.followIndex);
                    if (followIndexMetadata == null) {
                        listener.onFailure(new IllegalArgumentException("follow index [" + request.followIndex + "] does not exist"));
                    }

                    if (leaderIndexMetadata.getNumberOfShards() != followIndexMetadata.getNumberOfShards()) {
                        listener.onFailure(new IllegalArgumentException("leader index primary shards [" +
                                leaderIndexMetadata.getNumberOfShards() +  "] does not match with the number of " +
                                "shards of the follow index [" + followIndexMetadata.getNumberOfShards() + "]"));
                        return;
                    }

                    // TODO: other validation checks

                    final int numShards = followIndexMetadata.getNumberOfShards();
                    final AtomicInteger counter = new AtomicInteger(numShards);
                    final AtomicReferenceArray<Object> responses = new AtomicReferenceArray<>(followIndexMetadata.getNumberOfShards());
                    for (int i = 0; i < numShards; i++) {
                        final int shardId = i;
                        String taskId = followIndexMetadata.getIndexUUID() + "-" + shardId;
                        ShardFollowTask shardFollowTask =  new ShardFollowTask(new ShardId(followIndexMetadata.getIndex(), shardId),
                                new ShardId(leaderIndexMetadata.getIndex(), shardId));
                        persistentTasksService.startPersistentTask(taskId, ShardFollowTask.NAME, shardFollowTask,
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
                                        listener.onResponse(new Response());
                                    } else {
                                        // TODO: cancel all started tasks
                                        listener.onFailure(error);
                                    }
                                }
                            }

                        });
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

}
