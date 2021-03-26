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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;

public class GetGlobalCheckpointsAction extends ActionType<GetGlobalCheckpointsAction.Response> {

    public static final GetGlobalCheckpointsAction INSTANCE = new GetGlobalCheckpointsAction();
    // TODO: Do we still use xpack?
    public static final String NAME = "indices:monitor/xpack/fleet/global_checkpoints/";

    private GetGlobalCheckpointsAction() {
        super(NAME, GetGlobalCheckpointsAction.Response::new);
    }

    public static class Response extends ActionResponse {

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
            out.writeLongArray(globalCheckpoints);
        }
    }

    public static class Request extends ActionRequest implements IndicesRequest {

        private final String index;
        private final boolean waitForAdvance;
        private final long[] currentCheckpoints;

        Request(String index, boolean waitForAdvance, long[] currentCheckpoints) {
            this.index = index;
            this.waitForAdvance = waitForAdvance;
            this.currentCheckpoints = currentCheckpoints;
        }

        Request(StreamInput in) throws IOException {
            throw new AssertionError("GetGlobalCheckpointsAction should not be registered with the transport service.");
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public TimeValue pollTimeout() {
            return null;
        }

        public boolean waitForAdvance() {
            return waitForAdvance;
        }

        // TODO: Name
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

        @Inject
        public TransportGetGlobalCheckpointsAction(ActionFilters actionFilters, TaskManager taskManager, ClusterService clusterService) {
            super(NAME, actionFilters, taskManager);
            this.clusterService = clusterService;
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
            if (request.currentCheckpoints != null) {
                int actualNumberOfShards = request.currentCheckpoints.length;
                if (actualNumberOfShards != numberOfShards) {
                    listener.onFailure(new ElasticsearchException("Current checkpoints must equal number of shards. " +
                        "[Index Shard Count: " + numberOfShards + ", Current Checkpoints: " + actualNumberOfShards + "]"));
                    return;
                }
            }

            final AtomicArray<GetGlobalCheckpointAction.Response> responses = new AtomicArray<>(numberOfShards);
            final CountDown countDown = new CountDown(numberOfShards);
            for (int i = 0; i < numberOfShards; ++i) {

            }
        }
    }
}
