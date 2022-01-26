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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "cluster:monitor/health2"; // TODO: Need new name

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
            builder.field("timed_out", false);
            builder.field("status", "green");
            builder.field("cluster_name", clusterName.value());
            builder.array("impacts", Collections.emptyList());
            builder.startObject("components");
            for (Component component : components) {
                builder.startObject(component.getName());
                builder.field("status", component.getStatus());
                builder.startObject("indicators");
                List<Indicator> indicators = component.getIndicators();
                for (Indicator indicator : indicators) {
                    builder.field("name", indicator.getName());
                    builder.field("status", indicator.getStatus());
                    builder.field("explain", indicator.getExplain());
                    builder.startObject("meta");
                    indicator.toXContent(builder, params);
                    builder.endObject();
                    // TODO: Add detail / documentation
                }
                builder.endObject();
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
            listener.onResponse(new Response(clusterService.getClusterName(), Arrays.asList(component)));
        }
    }

    private interface Component {

        String getName();

        ClusterHealthStatus getStatus();

        List<Indicator> getIndicators();

    }

    private interface Indicator extends ToXContentFragment {

        String getName();

        ClusterHealthStatus getStatus();

        String getExplain();

    }

    private record NodeDoesNotHaveMaster(DiscoveryNode node) implements Indicator {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node-id", node.getId());
            builder.field("name-name", node.getName());
            return builder;
        }

        @Override
        public String getExplain() {
            return "health coordinating instance does not have master node";
        }

        @Override
        public String getName() {
            return "instance does not have master";
        }

        @Override
        public ClusterHealthStatus getStatus() {
            return ClusterHealthStatus.RED;
        }
    }

    private static class Controller implements Component {

        private final DiscoveryNode node;
        private final ClusterHealthStatus status;
        private final List<Indicator> indicators = new ArrayList<>(2);

        private Controller(final DiscoveryNode node, final ClusterState clusterState) {
            this.node = node;
            final DiscoveryNodes nodes = clusterState.nodes();
            final DiscoveryNode masterNode = nodes.getMasterNode();
            if (masterNode == null) {
                status = ClusterHealthStatus.RED;
                indicators.add(new NodeDoesNotHaveMaster(node));
            } else {
                status = ClusterHealthStatus.GREEN;
            }

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node-id", node.getId());
            builder.field("name-name", node.getName());
            return builder;
        }

        @Override
        public String getName() {
            return "controller";
        }

        @Override
        public ClusterHealthStatus getStatus() {
            return status;
        }

        @Override
        public List<Indicator> getIndicators() {
            return indicators;
        }
    }
}
