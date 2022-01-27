/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.components.backups.Backups;
import org.elasticsearch.health.components.controller.Controller;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    // TODO: Decide on name
    public static final String NAME = "cluster:internal/health";

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ClusterName clusterName;
        private final List<Component> components;

        public Response(ClusterName clusterName, List<Component> components) {
            this.clusterName = clusterName;
            this.components = components;
        }

        public Response(StreamInput in) {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("status", "green");
            builder.field("cluster_name", clusterName.value());
            builder.array("impacts");
            builder.startObject("components");
            for (Component component : components) {
                builder.field(component.getName());
                component.toXContent(builder, params);
            }
            builder.endObject();
            return builder.endObject();
        }
    }

    public static class Request extends ActionRequest {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, Response> {

        private final ClusterService clusterService;

        @Inject
        public TransportAction(
            final ActionFilters actionFilters,
            final TransportService transportService,
            final ClusterService clusterService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ClusterState clusterState = clusterService.state();
            final Controller component = new Controller(clusterService.localNode(), clusterState);
            listener.onResponse(new Response(clusterService.getClusterName(), Arrays.asList(component, new Backups())));
        }
    }

    public abstract static class Component implements ToXContentObject {

        public abstract String getName();

        public abstract ClusterHealthStatus getStatus();

        public abstract List<Indicator> getIndicators();

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("status", getStatus());
            builder.startArray("indicators");
            List<Indicator> indicators = getIndicators();
            for (Indicator indicator : indicators) {
                indicator.toXContent(builder, params);
            }
            builder.endArray();
            return builder.endObject();
        }

    }

    public abstract static class Indicator implements ToXContentObject {

        public abstract String getName();

        public abstract ClusterHealthStatus getStatus();

        public abstract String getExplain();

        public abstract void writeMeta(XContentBuilder builder, Params params) throws IOException;

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", getName());
            builder.field("status", getStatus());
            builder.field("explain", getExplain());
            builder.object("meta", xContentBuilder -> writeMeta(builder, params));
            // TODO: Add detail / documentation
            return builder.endObject();
        }
    }
}
