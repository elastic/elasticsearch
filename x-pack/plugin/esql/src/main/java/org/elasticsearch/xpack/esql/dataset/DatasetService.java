/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dataset;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Orchestrates create / replace / delete of datasets in cluster state. Dataset creation looks up
 * the parent {@link DataSource} in the same project to select the correct
 * {@link DataSourceValidator} for settings validation. Name collisions with indices, aliases,
 * data streams, and views are caught by {@code ProjectMetadata.Builder.build()} via
 * {@code ensureNoNameCollisions}, so no collision check lives here.
 */
public class DatasetService {

    // Operator-only: not exposed to end users. Change to Dynamic (+ ServerlessPublic) later to open.
    public static final Setting<Integer> MAX_DATASETS_COUNT_SETTING = Setting.intSetting(
        "esql.datasets.max_count",
        5_000,
        0,
        100_000,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    protected final ClusterService clusterService;
    private final Map<String, DataSourceValidator> validatorsByType;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    private volatile int maxDatasetsCount;

    public DatasetService(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType) {
        this.clusterService = clusterService;
        this.validatorsByType = Map.copyOf(validatorsByType);
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-dataset-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        clusterService.getClusterSettings().initializeAndWatch(MAX_DATASETS_COUNT_SETTING, v -> this.maxDatasetsCount = v);
    }

    protected DatasetMetadata getMetadata(ProjectMetadata projectMetadata) {
        return DatasetMetadata.get(projectMetadata);
    }

    /**
     * Create or replace a dataset. Parent data source lookup + validator dispatch run synchronously
     * before the cluster-state task. The task re-reads cluster state under CAS so a concurrent
     * data-source deletion racing a dataset PUT fails cleanly.
     */
    public void putDataset(ProjectId projectId, PutDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // 1. Look up parent data source directly from cluster state.
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DataSource parent = DataSourceMetadata.get(projectMetadata).get(request.dataSource());
        if (parent == null) {
            listener.onFailure(new ResourceNotFoundException("data source [{}] not found", request.dataSource()));
            return;
        }

        // 2. Validator dispatch via the parent type's validator.
        final DataSourceValidator validator = validatorsByType.get(parent.type());
        if (validator == null) {
            // This would indicate a cluster state with a data source whose type has no registered plugin — defensive.
            listener.onFailure(new IllegalStateException("no validator registered for data source type [" + parent.type() + "]"));
            return;
        }
        final Map<String, Object> validatedSettings;
        try {
            validatedSettings = validator.validateDataset(parent.settings(), request.resource(), request.rawSettings());
        } catch (ValidationException e) {
            listener.onFailure(e);
            return;
        }

        final Dataset dataset = new Dataset(
            request.name(),
            new DataSourceReference(request.dataSource()),
            request.resource(),
            request.description(),
            validatedSettings
        );

        // 3. No-op fast path.
        final Dataset existing = getMetadata(projectMetadata).get(dataset.name());
        if (dataset.equals(existing)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        // 4. Cluster-state update — re-check parent existence + count limit under CAS. Name collisions
        // with other abstractions are enforced by ProjectMetadata.Builder.build() via ensureNoNameCollisions.
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DatasetMetadata metadata = getMetadata(project);
                final Dataset current = metadata.get(dataset.name());
                if (dataset.equals(current)) {
                    return currentState; // another writer got here first with the same value
                }
                if (DataSourceMetadata.get(project).get(dataset.dataSource().getName()) == null) {
                    throw new ResourceNotFoundException("data source [{}] not found", dataset.dataSource().getName());
                }
                if (current == null && metadata.datasets().size() >= maxDatasetsCount) {
                    throw new IllegalArgumentException(
                        "cannot add dataset, the maximum number of datasets is reached: " + maxDatasetsCount
                    );
                }
                final Map<String, Dataset> updated = new HashMap<>(metadata.datasets());
                updated.put(dataset.name(), dataset);
                return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
            }
        };
        taskQueue.submitTask("update-esql-dataset-metadata-[" + dataset.name() + "]", task, task.timeout());
    }

    /** Delete a dataset by name. Surfaces 404 if the dataset doesn't exist at task-execution time. */
    public void deleteDataset(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String name,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(masterNodeTimeout, ackTimeout, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DatasetMetadata metadata = getMetadata(project);
                if (metadata.get(name) == null) {
                    throw new ResourceNotFoundException("dataset [{}] not found", name);
                }
                final Map<String, Dataset> updated = new HashMap<>(metadata.datasets());
                updated.remove(name);
                return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
            }
        };
        taskQueue.submitTask("delete-esql-dataset-metadata-[" + name + "]", task, task.timeout());
    }

    /** Single-name lookup against the current cluster state. */
    @Nullable
    public Dataset get(ProjectId projectId, String name) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return getMetadata(clusterService.state().metadata().getProject(projectId)).get(name);
    }

    /** List all dataset names in the project. */
    public Set<String> list(ProjectId projectId) {
        return getMetadata(clusterService.state().metadata().getProject(projectId)).datasets().keySet();
    }
}
