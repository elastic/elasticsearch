/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Set;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;
import org.elasticsearch.xpack.enrich.ExecuteEnrichPolicyTask;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction.Response;
import static org.elasticsearch.xpack.enrich.EnrichPolicyExecutor.TASK_ACTION;

/**
 * An internal action runs {@link org.elasticsearch.xpack.enrich.EnrichPolicyRunner} and ensures that:
 * <ul>
 *     <li>In case the cluster has more than one node, the policy runner isn't executed on the elected master
 *     <li>Additionally, if the cluster has master only nodes then the policy runner isn't executed on these nodes.
 * </ul>
 *
 * The {@link TransportExecuteEnrichPolicyAction} is a transport action that runs on the elected master node and
 * the actual policy execution may be heavy for the elected master node.
 * Although {@link org.elasticsearch.xpack.enrich.EnrichPolicyRunner} doesn't do heavy operations, the coordination
 * of certain operations may have a non-negligible overhead (for example the coordination of the reindex step).
 */
public class InternalExecutePolicyAction extends ActionType<Response> {

    private static final Logger LOGGER = LogManager.getLogger(InternalExecutePolicyAction.class);
    public static final InternalExecutePolicyAction INSTANCE = new InternalExecutePolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/internal_execute";

    private InternalExecutePolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ExecuteEnrichPolicyAction.Request {

        private final String enrichIndexName;

        public Request(String name, String enrichIndexName) {
            super(name);
            this.enrichIndexName = enrichIndexName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.enrichIndexName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(enrichIndexName);
        }

        public String getEnrichIndexName() {
            return enrichIndexName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Request request = (Request) o;
            return Objects.equals(enrichIndexName, request.enrichIndexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), enrichIndexName);
        }
    }

    public static class Transport extends HandledTransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final TransportService transportService;
        private final EnrichPolicyExecutor policyExecutor;
        private final AtomicInteger nodeGenerator = new AtomicInteger(Randomness.get().nextInt());

        @Inject
        public Transport(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            EnrichPolicyExecutor policyExecutor
        ) {
            super(NAME, transportService, actionFilters, Request::new);
            this.clusterService = clusterService;
            this.transportService = transportService;
            this.policyExecutor = policyExecutor;
        }

        @Override
        protected void doExecute(Task transportTask, Request request, ActionListener<Response> actionListener) {
            ClusterState clusterState = clusterService.state();
            DiscoveryNode targetNode = selectNodeForPolicyExecution(clusterState.nodes());
            if (clusterState.nodes().getLocalNode().equals(targetNode) == false) {
                ActionListenerResponseHandler<Response> handler = new ActionListenerResponseHandler<>(actionListener, Response::new);
                transportService.sendRequest(targetNode, NAME, request, handler);
                return;
            }

            // Can't use provided task, because in the case wait_for_completion=false then
            // as soon as actionListener#onResponse is invoked then the provided task get unregistered and
            // then there no way to see the policy execution in the list tasks or get task APIs.
            ExecuteEnrichPolicyTask task = (ExecuteEnrichPolicyTask) taskManager.register("enrich", TASK_ACTION, new TaskAwareRequest() {

                @Override
                public void setParentTask(TaskId taskId) {
                    request.setParentTask(taskId);
                }

                @Override
                public TaskId getParentTask() {
                    return request.getParentTask();
                }

                @Override
                public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    String description = "executing enrich policy ["
                        + request.getName()
                        + "] creating new enrich index ["
                        + request.getEnrichIndexName()
                        + "]";
                    return new ExecuteEnrichPolicyTask(id, type, action, description, parentTaskId, headers);
                }
            });

            try {
                ActionListener<ExecuteEnrichPolicyStatus> listener;
                if (request.isWaitForCompletion()) {
                    listener = ActionListener.wrap(result -> actionListener.onResponse(new Response(result)), actionListener::onFailure);
                } else {
                    listener = ActionListener.wrap(result -> LOGGER.debug("successfully executed policy [{}]", request.getName()), e -> {
                        if (e instanceof TaskCancelledException) {
                            LOGGER.info(e.getMessage());
                        } else {
                            LOGGER.error("failed to execute policy [" + request.getName() + "]", e);
                        }
                    });
                }
                policyExecutor.runPolicyLocally(task, request.getName(), request.getEnrichIndexName(), ActionListener.wrap(result -> {
                    taskManager.unregister(task);
                    listener.onResponse(result);
                }, e -> {
                    taskManager.unregister(task);
                    listener.onFailure(e);
                }));

                if (request.isWaitForCompletion() == false) {
                    TaskId taskId = new TaskId(clusterState.nodes().getLocalNodeId(), task.getId());
                    actionListener.onResponse(new Response(taskId));
                }
            } catch (Exception e) {
                taskManager.unregister(task);
                throw e;
            }
        }

        DiscoveryNode selectNodeForPolicyExecution(DiscoveryNodes discoNodes) {
            if (discoNodes.getIngestNodes().isEmpty()) {
                // if we don't fail here then reindex will fail with a more complicated error.
                // (EnrichPolicyRunner uses a pipeline with reindex)
                throw new IllegalStateException("no ingest nodes in this cluster");
            }
            // In case of a single node cluster:
            if (discoNodes.getSize() == 1) {
                return discoNodes.getLocalNode();
            }
            // This check exists to avoid redirecting potentially many times:
            if (discoNodes.isLocalNodeElectedMaster() == false) {
                // This method is first executed on the elected master node (via execute enrich policy action)
                // a node is picked and the request is redirected to that node.
                // Whatever node has been picked in the previous execution of the filters below should execute and
                // attempt not pick another node.
                return discoNodes.getLocalNode();
            }

            final DiscoveryNode[] nodes = discoNodes.getAllNodes()
                .stream()
                // filter out elected master node (which is the local node)
                .filter(discoNode -> discoNode.getId().equals(discoNodes.getMasterNodeId()) == false)
                // filter out dedicated master nodes
                .filter(discoNode -> discoNode.getRoles().equals(Set.of(DiscoveryNodeRole.MASTER_ROLE)) == false)
                // Filter out nodes that don't have this action yet
                .filter(discoNode -> discoNode.getVersion().onOrAfter(Version.V_7_15_0))
                .toArray(DiscoveryNode[]::new);
            if (nodes.length == 0) {
                throw new IllegalStateException("no suitable node was found to perform enrich policy execution");
            }
            return nodes[Math.floorMod(nodeGenerator.incrementAndGet(), nodes.length)];
        }
    }

}
