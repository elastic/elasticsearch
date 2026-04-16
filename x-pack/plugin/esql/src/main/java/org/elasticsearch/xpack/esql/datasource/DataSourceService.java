/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Orchestrates create / replace / delete of data sources in cluster state. Validator dispatch
 * runs before the cluster-state task so validation errors surface cleanly; the task re-validates
 * count limits under CAS to protect against concurrent writers.
 *
 * <p>Dataset referential integrity on delete ("refuse to delete a data source that has datasets
 * pointing at it") is enforced inside the delete task — same CAS window covers concurrent
 * dataset creation racing a data source deletion.
 */
public class DataSourceService {

    // Operator-only: not exposed to end users. Change to Dynamic (+ ServerlessPublic) later to open.
    public static final Setting<Integer> MAX_DATA_SOURCES_COUNT_SETTING = Setting.intSetting(
        "esql.data_sources.max_count",
        500,
        0,
        10_000,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    protected final ClusterService clusterService;
    private final Map<String, DataSourceValidator> validatorsByType;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    private volatile int maxDataSourcesCount;

    public DataSourceService(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType) {
        this.clusterService = clusterService;
        this.validatorsByType = Map.copyOf(validatorsByType);
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-data-source-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        clusterService.getClusterSettings().initializeAndWatch(MAX_DATA_SOURCES_COUNT_SETTING, v -> this.maxDataSourcesCount = v);
    }

    protected DataSourceMetadata getMetadata(ProjectMetadata projectMetadata) {
        return DataSourceMetadata.get(projectMetadata);
    }

    /**
     * Create or replace a data source. Validator dispatch (unknown type, field-level errors) returns
     * failure synchronously via {@code listener}; cluster-state mutation is deferred to the task queue.
     */
    public void putDataSource(ProjectId projectId, PutDataSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // 1. Validator dispatch — unknown type or validation error returns cleanly without touching cluster state.
        DataSourceValidator validator = validatorsByType.get(request.type());
        if (validator == null) {
            listener.onFailure(new IllegalArgumentException("unknown data source type [" + request.type() + "]"));
            return;
        }
        Map<String, org.elasticsearch.cluster.metadata.DataSourceSetting> validated;
        try {
            validated = validator.validateDatasource(request.rawSettings());
        } catch (ValidationException e) {
            listener.onFailure(e);
            return;
        }
        final DataSource dataSource = new DataSource(request.name(), request.type(), request.description(), validated);

        // 2. No-op fast path — if the already-present value equals what we're about to write, skip the task.
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DataSource existing = getMetadata(projectMetadata).get(dataSource.name());
        if (dataSource.equals(existing)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        // 3. Cluster-state update — re-validate under CAS for count limit.
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata metadata = getMetadata(project);
                final DataSource current = metadata.get(dataSource.name());
                if (dataSource.equals(current)) {
                    return currentState; // another writer got here first with the same value
                }
                if (current == null && metadata.dataSources().size() >= maxDataSourcesCount) {
                    throw new IllegalArgumentException(
                        "cannot add data source, the maximum number of data sources is reached: " + maxDataSourcesCount
                    );
                }
                final Map<String, DataSource> updated = new HashMap<>(metadata.dataSources());
                updated.put(dataSource.name(), dataSource);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("update-esql-data-source-metadata-[" + dataSource.name() + "]", task, task.timeout());
    }

    /**
     * Delete a data source by name. Referential integrity is enforced inside the task: the delete
     * fails with 409 if any dataset references this data source. A 404 is surfaced if the data source
     * doesn't exist at task-execution time.
     */
    public void deleteDataSource(
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
                final DataSourceMetadata metadata = getMetadata(project);
                if (metadata.get(name) == null) {
                    throw new ResourceNotFoundException("data source [{}] not found", name);
                }
                // Referential integrity: reject if any dataset references this data source.
                final DatasetMetadata datasets = DatasetMetadata.get(project);
                final List<String> dependents = datasets.datasets()
                    .values()
                    .stream()
                    .filter(ds -> name.equals(ds.dataSource().getName()))
                    .map(Dataset::name)
                    .toList();
                if (dependents.isEmpty() == false) {
                    throw new ElasticsearchStatusException(
                        "cannot delete data source [" + name + "]: referenced by datasets " + dependents,
                        RestStatus.CONFLICT
                    );
                }
                final Map<String, DataSource> updated = new HashMap<>(metadata.dataSources());
                updated.remove(name);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("delete-esql-data-source-metadata-[" + name + "]", task, task.timeout());
    }

    /** Single-name lookup against the current cluster state. */
    @Nullable
    public DataSource get(ProjectId projectId, String name) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return getMetadata(clusterService.state().metadata().getProject(projectId)).get(name);
    }

    /** List all data source names in the project. */
    public Set<String> list(ProjectId projectId) {
        return getMetadata(clusterService.state().metadata().getProject(projectId)).dataSources().keySet();
    }
}
