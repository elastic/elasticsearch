/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "cluster:monitor/health2"; // TODO: Need new name

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ClusterName clusterName;

        public Response(ClusterName clusterName) {
            this.clusterName = clusterName;
        }

        public Response(StreamInput in) throws IOException {
            this.clusterName = new ClusterName(in);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("timed_out", false);
            builder.field("status", "green");
            builder.field("cluster_name", clusterName.value());
            builder.array("impacts", Collections.emptyList());
            builder.startObject("components");
            builder.endObject();
            return builder.endObject();
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        public Request(StreamInput in) {

        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final ClusterService clusterService;

        @Inject
        public TransportAction(
            final ThreadPool threadPool,
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                NAME,
                false,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                Response::new,
                ThreadPool.Names.SAME
            );
            this.clusterService = clusterService;
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            // MasterNotDiscoveredException - if a timeout occurs
            // NodeClosedException - if the local node's cluster service closes
            // ConnectTransportException, NodeClosedException - Will retry these exceptions

            listener.onResponse(new Response(clusterService.getClusterName()));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // No blocks should inhibit this operation.
            return null;
        }
    }
}
