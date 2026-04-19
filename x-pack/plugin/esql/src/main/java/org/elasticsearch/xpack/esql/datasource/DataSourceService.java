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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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

    private static final Logger logger = LogManager.getLogger(DataSourceService.class);

    // Operator-only: not exposed to end users. Change to Dynamic (+ ServerlessPublic) later to open.
    // Data sources are admin-heavy config objects (one per external system, each carrying credentials
    // and operational responsibility). Intentionally tighter than the dataset cap: operators should
    // feel pressure to consolidate rather than proliferate. Revisited in esql-planning#502 against
    // cluster-state cost.
    public static final Setting<Integer> MAX_DATA_SOURCES_COUNT_SETTING = Setting.intSetting(
        "esql.data_sources.max_count",
        100,
        0,
        1_000,
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
            logger.warn("rejected put for data source [{}]: unknown type [{}]", request.name(), request.type());
            listener.onFailure(new IllegalArgumentException("unknown data source type [" + request.type() + "]"));
            return;
        }
        final Map<String, org.elasticsearch.cluster.metadata.DataSourceSetting> validated;
        try {
            validated = validator.validateDatasource(request.rawSettings());
        } catch (Exception e) {
            logger.warn(() -> "validator for type [" + request.type() + "] rejected put for data source [" + request.name() + "]", e);
            listener.onFailure(e);
            return;
        }
        final DataSource dataSource = new DataSource(request.name(), request.type(), request.description(), validated);

        // 2. No-op fast path — best-effort optimization. Reads local node state, which may lag the master.
        // A false miss triggers the task (which rechecks); a false hit returns TRUE on a stale match (safe).
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DataSource existing = getMetadata(projectMetadata).get(dataSource.name());
        if (dataSource.equals(existing)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        // 3. Cluster-state update — re-validate under CAS for count limit.
        logger.debug("submitting put data source [{}] of type [{}]", dataSource.name(), dataSource.type());
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
                    logger.warn("rejected put for data source [{}]: maximum count [{}] reached", dataSource.name(), maxDataSourcesCount);
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
     *
     * <p>The delete-vs-dataset-put race is serialized by {@code MasterService}, not by shared task
     * queuing — the data source queue and the dataset queue are distinct. {@code MasterService}
     * applies cluster-state updates one at a time, so whichever task runs second sees the other's
     * effect: a dataset-put task racing a data-source-delete will either create the dataset before
     * the delete observes it (409 on delete) or find the parent gone when it executes
     * ({@code ResourceNotFoundException} in {@code DatasetService.putDataset}).
     */
    public void deleteDataSource(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String name,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug("submitting delete data source [{}]", name);
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
                    logger.warn("rejected delete for data source [{}]: referenced by datasets {}", name, dependents);
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
