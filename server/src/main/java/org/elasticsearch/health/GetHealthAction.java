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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "cluster:monitor/health_api";

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ClusterName clusterName;
        private final HealthStatus status;
        private final List<HealthComponentResult> components;

        public Response(StreamInput in) {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        public Response(final ClusterName clusterName, final List<HealthComponentResult> components) {
            this.clusterName = clusterName;
            this.components = components;
            this.status = HealthStatus.merge(components.stream().map(HealthComponentResult::status));
        }

        public ClusterName getClusterName() {
            return clusterName;
        }

        public HealthStatus getStatus() {
            return status;
        }

        public List<HealthComponentResult> getComponents() {
            return components;
        }

        public HealthComponentResult findComponent(String name) {
            return components.stream()
                .filter(c -> Objects.equals(c.name(), name))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Component [" + name + "] is not found"));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("status", status.xContentValue());
            builder.field("cluster_name", clusterName.value());
            builder.startObject("components");
            for (HealthComponentResult component : components) {
                builder.field(component.name(), component, params);
            }
            builder.endObject();
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response response = (Response) o;
            return clusterName.equals(response.clusterName) && status == response.status && components.equals(response.components);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, status, components);
        }

        @Override
        public String toString() {
            return "Response{clusterName=" + clusterName + ", status=" + status + ", components=" + components + '}';
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
        private final HealthService healthService;

        @Inject
        public TransportAction(
            ActionFilters actionFilters,
            TransportService transportService,
            ClusterService clusterService,
            HealthService healthService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
            this.healthService = healthService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            listener.onResponse(new Response(clusterService.getClusterName(), healthService.getHealth()));
        }
    }
}
