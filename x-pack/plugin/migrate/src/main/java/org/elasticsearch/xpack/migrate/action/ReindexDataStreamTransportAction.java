/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.MinHashIndexParamBounds;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTaskParams;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate.getReindexRequiredPredicate;
import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.TASK_ID_PREFIX;

/*
 * This transport action creates a new persistent task for reindexing the source data stream given in the request. On successful creation
 *  of the persistent task, it responds with the persistent task id so that the user can monitor the persistent task.
 */
public class ReindexDataStreamTransportAction extends HandledTransportAction<ReindexDataStreamRequest, AcknowledgedResponse> {
    private final PersistentTasksService persistentTasksService;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public ReindexDataStreamTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService,
        ClusterService clusterService,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(
            ReindexDataStreamAction.NAME,
            true,
            transportService,
            actionFilters,
            ReindexDataStreamRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.transportService = transportService;
        this.persistentTasksService = persistentTasksService;
        this.clusterService = clusterService;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<AcknowledgedResponse> listener) {
        String sourceDataStreamName = request.getSourceDataStream();
        final var projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
        DataStream dataStream = projectMetadata == null ? null : projectMetadata.dataStreams().get(sourceDataStreamName);
        if (dataStream == null) {
            listener.onFailure(new ResourceNotFoundException("Data stream named [{}] does not exist", sourceDataStreamName));
            return;
        }

        int totalIndices = dataStream.getIndices().size();
        List<Index> indicesToBeUpgraded = dataStream.getIndices()
            .stream()
            .filter(getReindexRequiredPredicate(projectMetadata, false, dataStream.isSystem()))
            .toList();

        try {
            reindexCompatibilityCheck(sourceDataStreamName, indicesToBeUpgraded, projectMetadata);
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }

        int totalIndicesToBeUpgraded = indicesToBeUpgraded.size();
        ReindexDataStreamTaskParams params = new ReindexDataStreamTaskParams(
            sourceDataStreamName,
            transportService.getThreadPool().absoluteTimeInMillis(),
            totalIndices,
            totalIndicesToBeUpgraded,
            ClientHelper.getPersistableSafeSecurityHeaders(transportService.getThreadPool().getThreadContext(), clusterService.state())
        );
        final var projectId = projectMetadata.id();
        final var persistentTaskId = getPersistentTaskId(sourceDataStreamName);
        final var persistentTask = PersistentTasksCustomMetadata.getTaskWithId(projectMetadata, persistentTaskId);
        if (persistentTask == null) {
            startTask(projectId, listener, persistentTaskId, params);
        } else {
            GetMigrationReindexStatusAction.Request statusRequest = new GetMigrationReindexStatusAction.Request(sourceDataStreamName);
            statusRequest.setParentTask(task.getParentTaskId());
            client.execute(
                GetMigrationReindexStatusAction.INSTANCE,
                statusRequest,
                listener.delegateFailureAndWrap((getListener, getResponse) -> {
                    if (getResponse.getEnrichedStatus().complete() == false) {
                        throw new ResourceAlreadyExistsException("Reindex task for data stream [{}] already exists", sourceDataStreamName);
                    }
                    CancelReindexDataStreamAction.Request cancelRequest = new CancelReindexDataStreamAction.Request(sourceDataStreamName);
                    cancelRequest.setParentTask(task.getParentTaskId());
                    client.execute(
                        CancelReindexDataStreamAction.INSTANCE,
                        cancelRequest,
                        getListener.delegateFailureAndWrap(
                            (cancelListener, cancelResponse) -> startTask(projectId, cancelListener, persistentTaskId, params)
                        )
                    );
                })
            );
        }

    }

    /**
     * Throws if any of the indices has settings that the new index created by its reindex would reject.
     */
    static void reindexCompatibilityCheck(String dataStreamName, List<Index> indicesToBeUpgraded, ProjectMetadata projectMetadata) {
        // The destination of each reindex copies its source's settings, so fail now rather than partway through the migration
        // if a min_hash filter that new indices reject would be copied.
        List<String> minHashViolations = new ArrayList<>();
        for (Index index : indicesToBeUpgraded) {
            List<String> violations = MinHashIndexParamBounds.indexSettingsViolations(projectMetadata.index(index).getSettings());
            if (violations.isEmpty() == false) {
                minHashViolations.add("index [" + index.getName() + "] " + String.join(" ", violations));
            }
        }

        if (minHashViolations.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Reindexing data stream ["
                    + dataStreamName
                    + "] would fail: backing indices define [min_hash] filters that new indices reject: "
                    + String.join(" ", minHashViolations)
                    + " "
                    + MinHashIndexParamBounds.REINDEX_REMEDIATION
            );
        }
    }

    private void startTask(
        ProjectId projectId,
        ActionListener<AcknowledgedResponse> listener,
        String persistentTaskId,
        ReindexDataStreamTaskParams params
    ) {
        persistentTasksService.sendProjectStartRequest(
            projectId,
            persistentTaskId,
            ReindexDataStreamTask.TASK_NAME,
            params,
            TimeValue.THIRTY_SECONDS /* TODO should this be configurable? longer by default? infinite? */,
            ActionListener.wrap(startedTask -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
        );
    }

    private String getPersistentTaskId(String dataStreamName) throws ResourceAlreadyExistsException {
        return TASK_ID_PREFIX + dataStreamName;
    }
}
