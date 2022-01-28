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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.components.controller.Controller;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    // TODO: Decide on name
    public static final String NAME = "cluster:monitor/health/get";

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ClusterName clusterName;
        private final ClusterHealthStatus status;
        private final int numberOfNodes;
        private final int numberOfDataNodes;
        private final String masterNodeId;
        private final List<Component> components;

        public Response(StreamInput in) {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        public Response(
            final ClusterName clusterName,
            final int numberOfNodes,
            final int numberOfDataNodes,
            final String masterNodeId,
            final List<Component> components
        ) {
            this.clusterName = clusterName;
            this.numberOfNodes = numberOfNodes;
            this.numberOfDataNodes = numberOfDataNodes;
            this.masterNodeId = masterNodeId;
            this.components = components;
            ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
            for (Component component : components) {
                if (component.status().equals(ClusterHealthStatus.RED)) {
                    computeStatus = ClusterHealthStatus.RED;
                    break;
                } else if (component.status().equals(ClusterHealthStatus.YELLOW)) {
                    computeStatus = ClusterHealthStatus.YELLOW;
                }
            }
            this.status = computeStatus;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }

        public ClusterHealthStatus getStatus() {
            return status;
        }

        public int getNumberOfNodes() {
            return numberOfNodes;
        }

        public int getNumberOfDataNodes() {
            return numberOfDataNodes;
        }

        public String getMasterNodeId() {
            return masterNodeId;
        }

        public List<Component> getComponents() {
            return components;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("status", status);
            builder.field("cluster_name", clusterName.value());
            builder.field("number_of_nodes", numberOfNodes);
            builder.field("number_of_data_nodes", numberOfDataNodes);
            builder.field("master_node_id", masterNodeId);
            builder.array("impacts");
            builder.startObject("components");
            for (Component component : components) {
                builder.field(component.name());
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
            final Component controller = Controller.createControllerComponent(clusterService.localNode(), clusterState);
            final Component snapshots = new Component("snapshots", ClusterHealthStatus.GREEN, Collections.emptyList());
            final ClusterName clusterName = clusterService.getClusterName();
            final int numberOfNodes = clusterState.nodes().getSize();
            final int numberOfDataNodes = clusterState.nodes().getDataNodes().size();
            final DiscoveryNode masterNode = clusterState.nodes().getMasterNode();
            final String masterNodeId = masterNode == null ? null : masterNode.getId();
            listener.onResponse(
                new Response(clusterName, numberOfNodes, numberOfDataNodes, masterNodeId, Arrays.asList(controller, snapshots))
            );
        }
    }

    public record Component(String name, ClusterHealthStatus status, List<Indicator> indicators) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("status", status);
            builder.startArray("indicators");
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
