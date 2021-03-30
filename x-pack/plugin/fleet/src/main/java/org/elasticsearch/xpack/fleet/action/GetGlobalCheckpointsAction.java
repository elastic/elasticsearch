/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class GetGlobalCheckpointsAction extends ActionType<GetGlobalCheckpointsAction.Response> {

    public static final GetGlobalCheckpointsAction INSTANCE = new GetGlobalCheckpointsAction();
    public static final String NAME = "indices:monitor/fleet/global_checkpoints";

    private GetGlobalCheckpointsAction() {
        super(NAME, GetGlobalCheckpointsAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final long[] globalCheckpoints;

        public Response(long[] globalCheckpoints) {
            this.globalCheckpoints = globalCheckpoints;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            globalCheckpoints = in.readLongArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetGlobalCheckpointsAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.array("global_checkpoints", globalCheckpoints);
            return builder.endObject();
        }
    }

    public static class Request extends ActionRequest implements IndicesRequest {

        private final String index;
        private final boolean waitForAdvance;
        private final long[] currentCheckpoints;
        private final TimeValue pollTimeout;

        public Request(String index, boolean waitForAdvance, long[] currentCheckpoints, TimeValue pollTimeout) {
            this.index = index;
            this.waitForAdvance = waitForAdvance;
            this.currentCheckpoints = currentCheckpoints;
            this.pollTimeout = pollTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public TimeValue pollTimeout() {
            return pollTimeout;
        }

        public boolean waitForAdvance() {
            return waitForAdvance;
        }

        public long[] currentCheckpoints() {
            return currentCheckpoints;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

    public static class TransportGetGlobalCheckpointsAction extends TransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final NodeClient client;

        @Inject
        public TransportGetGlobalCheckpointsAction(final ActionFilters actionFilters, final TransportService transportService,
                                                   final ClusterService clusterService, final NodeClient client) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final IndexMetadata indexMetadata = clusterService.state().getMetadata().index(request.index);

            if (indexMetadata == null) {
                // Index not found
                listener.onFailure(new IndexNotFoundException(request.index));
                return;
            }

            final int numberOfShards = indexMetadata.getNumberOfShards();
            final long[] currentCheckpoints;
            final int currentCheckpointCount = request.currentCheckpoints().length;
            if (currentCheckpointCount != 0) {
                if (currentCheckpointCount != numberOfShards) {
                    listener.onFailure(new ElasticsearchException("current_checkpoints must equal number of shards. " +
                        "[shard count: " + numberOfShards + ", current_checkpoints: " + currentCheckpointCount + "]"));
                    return;
                }
                currentCheckpoints = request.currentCheckpoints();
            } else {
                currentCheckpoints = new long[numberOfShards];
                for (int i = 0; i < numberOfShards; ++i) {
                    currentCheckpoints[i] = SequenceNumbers.NO_OPS_PERFORMED;
                }
            }

            final AtomicArray<GetGlobalCheckpointAction.Response> responses = new AtomicArray<>(numberOfShards);
            final CountDown countDown = new CountDown(numberOfShards);
            for (int i = 0; i < numberOfShards; ++i) {
                final int shardIndex = i;
                GetGlobalCheckpointAction.Request shardChangesRequest =
                    new GetGlobalCheckpointAction.Request(new ShardId(indexMetadata.getIndex(), shardIndex), request.waitForAdvance(),
                        currentCheckpoints[shardIndex], request.pollTimeout());

                client.execute(GetGlobalCheckpointAction.INSTANCE, shardChangesRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(GetGlobalCheckpointAction.Response response) {
                        responses.set(shardIndex, response);
                        if (countDown.countDown()) {
                            long[] globalCheckpoints = new long[responses.length()];
                            int i = 0;
                            for (GetGlobalCheckpointAction.Response r : responses.asList()) {
                                globalCheckpoints[i++] = r.getGlobalCheckpoint();
                            }
                            listener.onResponse(new Response(globalCheckpoints));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (countDown.fastForward()) {
                            listener.onFailure(e);
                        }
                    }
                });
            }
        }
    }
}
