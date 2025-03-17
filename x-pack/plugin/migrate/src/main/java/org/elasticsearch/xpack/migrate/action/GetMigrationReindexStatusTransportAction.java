/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Request;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Response;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamEnrichedStatus;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class GetMigrationReindexStatusTransportAction extends HandledTransportAction<Request, Response> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;

    @Inject
    public GetMigrationReindexStatusTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(GetMigrationReindexStatusAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        String index = request.getIndex();
        String persistentTaskId = ReindexDataStreamAction.TASK_ID_PREFIX + index;
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = PersistentTasksCustomMetadata.getTaskWithId(
            clusterService.state(),
            persistentTaskId
        );
        if (persistentTask == null) {
            listener.onFailure(new ResourceNotFoundException("No migration reindex status found for [{}]", index));
        } else if (persistentTask.isAssigned()) {
            String nodeId = persistentTask.getExecutorNode();
            if (clusterService.localNode().getId().equals(nodeId)) {
                fetchAndReportStatusForTaskOnThisNode(persistentTaskId, listener);
            } else {
                fetchAndReportStatusForTaskOnRemoteNode(task, request, nodeId, listener);
            }
        } else {
            listener.onFailure(new ElasticsearchException("Persistent task with id [{}] is not assigned to a node", persistentTaskId));
        }
    }

    private Task getRunningPersistentTaskFromTaskManager(String persistentTaskId) {
        Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getType().equals("persistent"))
            .filter(
                entry -> entry.getValue() instanceof AllocatedPersistentTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) entry.getValue()).getPersistentTaskId()))
            )
            .findAny();
        return optionalTask.<Task>map(Map.Entry::getValue).orElse(null);
    }

    void fetchAndReportStatusForTaskOnThisNode(String persistentTaskId, ActionListener<Response> listener) {
        Task runningTask = getRunningPersistentTaskFromTaskManager(persistentTaskId);
        if (runningTask == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [%s] is supposed to be running on node [%s], but the task is not found on that node",
                        persistentTaskId,
                        clusterService.localNode().getId()
                    )
                )
            );
        } else {
            TaskInfo info = runningTask.taskInfo(clusterService.localNode().getId(), true);
            ReindexDataStreamStatus status = (ReindexDataStreamStatus) info.status();
            Set<String> inProgressIndices = status.inProgress();
            if (inProgressIndices.isEmpty()) {
                // We have no reason to fetch index stats since there are no in progress indices
                reportStatus(Map.of(), status, listener);
            } else {
                fetchInProgressStatsAndReportStatus(inProgressIndices, status, listener);
            }
        }
    }

    /*
     * The status is enriched with the information from inProgressMap to create a new ReindexDataStreamEnrichedStatus, which is used in the
     * response sent to the listener.
     */
    private void reportStatus(
        Map<String, Tuple<Long, Long>> inProgressMap,
        ReindexDataStreamStatus status,
        ActionListener<Response> listener
    ) {
        ReindexDataStreamEnrichedStatus enrichedStatus = new ReindexDataStreamEnrichedStatus(
            status.persistentTaskStartTime(),
            status.totalIndices(),
            status.totalIndicesToBeUpgraded(),
            status.complete(),
            status.exception(),
            inProgressMap,
            status.pending(),
            status.errors()
        );
        listener.onResponse(new Response(enrichedStatus));
    }

    /*
     * This method fetches doc counts for all indices in inProgressIndices (and the indices they are being reindexed into). After
     * successfully fetching those, reportStatus is called.
     */
    private void fetchInProgressStatsAndReportStatus(
        Set<String> inProgressIndices,
        ReindexDataStreamStatus status,
        ActionListener<Response> listener
    ) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        String[] indices = inProgressIndices.stream()
            .flatMap(index -> Stream.of(index, ReindexDataStreamIndexTransportAction.generateDestIndexName(index)))
            .toList()
            .toArray(new String[0]);
        indicesStatsRequest.indices(indices);
        /*
         * It is possible that the destination index will not exist yet, so we want to ignore the fact that it is missing
         */
        indicesStatsRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true));
        client.execute(IndicesStatsAction.INSTANCE, indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                Map<String, Tuple<Long, Long>> inProgressMap = new HashMap<>();
                for (String index : inProgressIndices) {
                    IndexStats sourceIndexStats = indicesStatsResponse.getIndex(index);
                    final long totalDocsInIndex;
                    if (sourceIndexStats == null) {
                        totalDocsInIndex = 0;
                    } else {
                        DocsStats totalDocsStats = sourceIndexStats.getPrimaries().getDocs();
                        totalDocsInIndex = totalDocsStats == null ? 0 : totalDocsStats.getCount();
                    }
                    IndexStats migratedIndexStats = indicesStatsResponse.getIndex(
                        ReindexDataStreamIndexTransportAction.generateDestIndexName(index)
                    );
                    final long reindexedDocsInIndex;
                    if (migratedIndexStats == null) {
                        reindexedDocsInIndex = 0;
                    } else {
                        DocsStats reindexedDocsStats = migratedIndexStats.getPrimaries().getDocs();
                        reindexedDocsInIndex = reindexedDocsStats == null ? 0 : reindexedDocsStats.getCount();
                    }
                    inProgressMap.put(index, Tuple.tuple(totalDocsInIndex, reindexedDocsInIndex));
                }
                reportStatus(inProgressMap, status, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /*
     * The task and its status exist on some other node, so this method forwards the request to that node.
     */
    private void fetchAndReportStatusForTaskOnRemoteNode(Task thisTask, Request request, String nodeId, ActionListener<Response> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [%s] is supposed to be running on node [%s], but that node is not part of the cluster",
                        request.getIndex(),
                        nodeId
                    )
                )
            );
        } else {
            Request nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
            transportService.sendRequest(
                node,
                GetMigrationReindexStatusAction.NAME,
                nodeRequest,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, Response::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
