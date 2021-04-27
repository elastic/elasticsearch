/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class TransformNodes {

    private TransformNodes() {}

    /**
     * Get node assignments for a given list of transforms.
     *
     * @param transformIds The transforms.
     * @param clusterState State
     * @return The {@link TransformNodeAssignments} for the given transforms.
     */
    public static TransformNodeAssignments transformTaskNodes(List<String> transformIds, ClusterState clusterState) {
        Set<String> executorNodes = new HashSet<>();
        Set<String> assigned = new HashSet<>();
        Set<String> waitingForAssignment = new HashSet<>();

        PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);

        if (tasksMetadata != null) {
            Set<String> transformIdsSet = new HashSet<>(transformIds);

            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = tasksMetadata.findTasks(
                TransformField.TASK_NAME,
                t -> transformIdsSet.contains(t.getId())
            );

            for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasks) {
                if (task.isAssigned()) {
                    executorNodes.add(task.getExecutorNode());
                    assigned.add(task.getId());
                } else {
                    waitingForAssignment.add(task.getId());
                }
            }
        }

        Set<String> stopped = transformIds.stream()
            .filter(id -> (assigned.contains(id) || waitingForAssignment.contains(id)) == false)
            .collect(Collectors.toSet());

        return new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);
    }

    /**
     * Get node assignments for a given transform pattern.
     *
     * Note: This only returns p-task assignments, stopped transforms are not reported. P-Tasks can be running or waiting for a node.
     *
     * @param transformId The transform or a wildcard pattern, including '_all' to match against transform tasks.
     * @param clusterState State
     * @return The {@link TransformNodeAssignments} for the given pattern.
     */
    public static TransformNodeAssignments findPersistentTasks(String transformId, ClusterState clusterState) {
        Set<String> executorNodes = new HashSet<>();
        Set<String> assigned = new HashSet<>();
        Set<String> waitingForAssignment = new HashSet<>();

        PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);

        if (tasksMetadata != null) {
            Predicate<PersistentTask<?>> taskMatcher = Strings.isAllOrWildcard(new String[] { transformId }) ? t -> true : t -> {
                TransformTaskParams transformParams = (TransformTaskParams) t.getParams();
                return Regex.simpleMatch(transformId, transformParams.getId());
            };

            for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasksMetadata.findTasks(TransformField.TASK_NAME, taskMatcher)) {
                if (task.isAssigned()) {
                    executorNodes.add(task.getExecutorNode());
                    assigned.add(task.getId());
                } else {
                    waitingForAssignment.add(task.getId());
                }
            }
        }
        return new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, Collections.emptySet());
    }

    /**
     * Get the assignment of a specific transform.
     *
     * @param transformId the transform id
     * @param clusterState state
     * @return {@link Assignment} of task
     */
    public static Assignment getAssignment(String transformId, ClusterState clusterState) {
        PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);
        PersistentTask<?> task = tasksMetadata.getTask(transformId);

        if (task != null) {
            return task.getAssignment();
        }

        return PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT;
    }

    /**
     * Get the number of transform nodes in the cluster
     *
     * @param nodes nodes to examine
     * @return number of transform nodes
     */
    public static boolean hasAnyTransformNode(DiscoveryNodes nodes) {
        return StreamSupport.stream(nodes.spliterator(), false)
            .anyMatch(node -> node.getRoles().contains(DiscoveryNodeRole.TRANSFORM_ROLE));
    }

    /**
     * Check if cluster has at least 1 transform nodes and add a header warning if not.
     * To be used by transport actions only.
     * Don't do this if a reset is in progress, because the feature reset API touches
     * all features even if they have never been used.
     *
     * @param clusterState state
     */
    public static void warnIfNoTransformNodes(ClusterState clusterState) {
        if (TransformMetadata.getTransformMetadata(clusterState).isResetMode() == false) {
            if (hasAnyTransformNode(clusterState.getNodes()) == false) {
                HeaderWarning.addWarning(TransformMessages.REST_WARN_NO_TRANSFORM_NODES);
            }
        }
    }

    /**
     * Check if cluster has at least 1 transform nodes and throw an exception if not.
     * To be used by transport actions only.
     *
     * @param clusterState state
     */
    public static void throwIfNoTransformNodes(ClusterState clusterState) {
        if (hasAnyTransformNode(clusterState.getNodes()) == false) {
            throw ExceptionsHelper.badRequestException(TransformMessages.REST_WARN_NO_TRANSFORM_NODES);
        }
    }

    public static <Request extends TransportRequest, Response extends TransportResponse> boolean redirectToAnotherNodeIfNeeded(
        ClusterState clusterState,
        Settings nodeSettings,
        boolean requiresRemote,
        TransportService transportService,
        String actionName,
        Request request,
        Writeable.Reader<Response> reader,
        ActionListener<Response> listener
    ) {
        final boolean isTransformNode = DiscoveryNode.hasRole(nodeSettings, DiscoveryNodeRole.TRANSFORM_ROLE);
        final boolean isRemoteClusterClientNode = DiscoveryNode.isRemoteClusterClient(nodeSettings);
        final DiscoveryNodes nodes = clusterState.nodes();

        if ((isTransformNode == false) || (requiresRemote && (isRemoteClusterClientNode == false))) {
            Optional<DiscoveryNode> appropriateNode = selectAnyNodeThatCanRunThisTransform(nodes, requiresRemote);
            if (appropriateNode.isPresent()) {
                // Redirect the request to an appropriate node
                transportService.sendRequest(
                    appropriateNode.get(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(listener, reader));
            } else {
                Map<String, String> explain = new TreeMap<>();
                for (DiscoveryNode node : nodes) {
                    nodeCanRunThisTransform(node, Version.V_7_13_0, requiresRemote, explain);
                }
                // There are no appropriate nodes in the cluster, fail
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "No appropriate node to run on, reasons [{}]",
                        explain.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining("|"))));
            }
            return true;
        }
        return false;
    }

    /**
     * Select any node among provided nodes that satisfies all of the following:
     *  - is a transform node
     *  - is a remote_cluster_client node (in case this transform uses CCS, i.e. requires access to remote indices)
     *  - runs at least version 7.13
     *    This is needed as version 7.13 contains changes in wire format of {@code TransformDestIndexSettings} which are needed to correctly
     *    read the redirected response.
     *
     * @param nodes nodes to select from
     * @param requiresRemote whether this transform requires access to remote indices
     * @return selected node or {@code Optional.empty()} if none of the nodes satisfy the conditions
     */
    static Optional<DiscoveryNode> selectAnyNodeThatCanRunThisTransform(DiscoveryNodes nodes, boolean requiresRemote) {
        return StreamSupport.stream(nodes.spliterator(), false)
            .filter(node -> nodeCanRunThisTransform(node, Version.V_7_13_0, requiresRemote, null))
            .findAny();
    }

    public static boolean nodeCanRunThisTransform(DiscoveryNode node,
                                                  Version minRequiredVersion,
                                                  boolean requiresRemote,
                                                  Map<String, String> explain) {
        // version of the transform run on a node that has at least the same version
        if (node.getVersion().onOrAfter(minRequiredVersion) == false) {
            if (explain != null) {
                explain.put(
                    node.getId(), "node has version: " + node.getVersion() + " but transform requires at least " + minRequiredVersion);
            }
            return false;
        }

        // transform enabled?
        if (node.getRoles().contains(DiscoveryNodeRole.TRANSFORM_ROLE) == false) {
            if (explain != null) {
                explain.put(node.getId(), "not a transform node");
            }
            return false;
        }

        // does the transform require a remote and remote is enabled?
        if (requiresRemote && node.isRemoteClusterClient() == false) {
            if (explain != null) {
                explain.put(node.getId(), "transform requires a remote connection but remote is disabled");
            }
            return false;
        }

        // we found no reason that the transform can not run on this node
        return true;
    }
}
