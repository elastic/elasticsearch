/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.dataset;

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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Orchestrates create / replace / delete of datasets in cluster state. */
public class DatasetService {

    private static final Logger logger = LogManager.getLogger(DatasetService.class);

    // Operator-only. Validation of the chosen defaults + ceiling is tracked at esql-planning#502.
    public static final Setting<Integer> MAX_DATASETS_COUNT_SETTING = Setting.intSetting(
        "esql.datasets.max_count",
        1_000,
        0,
        10_000,
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
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DataSource parent = DataSourceMetadata.get(projectMetadata).get(request.dataSource());
        if (parent == null) {
            logger.warn("rejected put for dataset [{}]: parent data source [{}] not found", request.name(), request.dataSource());
            listener.onFailure(new ResourceNotFoundException("data source [{}] not found", request.dataSource()));
            return;
        }

        final DataSourceValidator validator = validatorsByType.get(parent.type());
        if (validator == null) {
            logger.warn("rejected put for dataset [{}]: no validator for parent type [{}]", request.name(), parent.type());
            listener.onFailure(new IllegalStateException("no validator registered for data source type [" + parent.type() + "]"));
            return;
        }
        final Map<String, Object> validatedSettings;
        try {
            validatedSettings = validator.validateDataset(parent.settings(), request.resource(), request.rawSettings());
        } catch (Exception e) {
            logger.warn(() -> "validator for type [" + parent.type() + "] rejected put for dataset [" + request.name() + "]", e);
            listener.onFailure(e);
            return;
        }

        final Dataset probeDataset = new Dataset(
            request.name(),
            new DataSourceReference(request.dataSource()),
            request.resource(),
            request.description(),
            validatedSettings
        );
        final Dataset existing = getMetadata(projectMetadata).get(probeDataset.name());
        if (probeDataset.equals(existing)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        logger.debug("submitting put dataset [{}] with parent [{}]", request.name(), request.dataSource());
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return executePutDatasetTaskBody(currentState, projectId, request);
            }
        };
        taskQueue.submitTask("update-esql-dataset-metadata-[" + request.name() + "]", task, task.timeout());
    }

    // Extracted so unit tests can drive the task body directly against fabricated cluster-state snapshots.
    ClusterState executePutDatasetTaskBody(ClusterState currentState, ProjectId projectId, PutDatasetAction.Request request) {
        final ProjectMetadata project = currentState.metadata().getProject(projectId);
        final DataSource currentParent = DataSourceMetadata.get(project).get(request.dataSource());
        if (currentParent == null) {
            throw new ResourceNotFoundException("data source [{}] not found", request.dataSource());
        }
        // Re-dispatch: parent's type may have changed (delete+recreate) since pre-task validation.
        final DataSourceValidator currentValidator = validatorsByType.get(currentParent.type());
        if (currentValidator == null) {
            throw new IllegalStateException("no validator registered for data source type [" + currentParent.type() + "]");
        }
        final Map<String, Object> freshSettings = currentValidator.validateDataset(
            currentParent.settings(),
            request.resource(),
            request.rawSettings()
        );
        final Dataset dataset = new Dataset(
            request.name(),
            new DataSourceReference(request.dataSource()),
            request.resource(),
            request.description(),
            freshSettings
        );
        final DatasetMetadata metadata = getMetadata(project);
        final Dataset current = metadata.get(dataset.name());
        if (dataset.equals(current)) {
            return currentState; // another writer got here first with the same value
        }
        if (current == null && metadata.datasets().size() >= maxDatasetsCount) {
            logger.warn("rejected put for dataset [{}]: maximum count [{}] reached", dataset.name(), maxDatasetsCount);
            throw new IllegalArgumentException("cannot add dataset, the maximum number of datasets is reached: " + maxDatasetsCount);
        }
        final Map<String, Dataset> updated = new HashMap<>(metadata.datasets());
        updated.put(dataset.name(), dataset);
        return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
    }

    /** Delete a dataset by name. Surfaces 404 if the dataset doesn't exist at task-execution time. */
    public void deleteDataset(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String name,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug("submitting delete dataset [{}]", name);
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
