/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.ConfigChangeTelemetry;
import org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator;
import org.elasticsearch.xpack.esql.datasources.MaxDatasetsCountException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Orchestrates create / replace / delete of datasets in cluster state. */
public class DatasetService {

    private static final Logger logger = LogManager.getLogger(DatasetService.class);

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
    private final ExternalSourceMetrics metrics;

    private volatile int maxDatasetsCount;

    public DatasetService(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType) {
        this(clusterService, validatorsByType, ExternalSourceMetrics.NOOP);
    }

    public DatasetService(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType, ExternalSourceMetrics metrics) {
        this.clusterService = clusterService;
        this.validatorsByType = Map.copyOf(validatorsByType);
        this.metrics = metrics == null ? ExternalSourceMetrics.NOOP : metrics;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-dataset-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        // The ceiling is watched while the setting exists, which is while the federation feature is registered (see
        // Federation#settings). Where the feature is unregistered the ceiling is the setting's default and cannot be
        // changed, which no request observes because the CRUD REST routes are not registered either.
        clusterService.getClusterSettings().initializeAndWatchIfRegistered(MAX_DATASETS_COUNT_SETTING, v -> this.maxDatasetsCount = v);
    }

    protected DatasetMetadata getMetadata(ProjectMetadata projectMetadata) {
        return DatasetMetadata.get(projectMetadata);
    }

    /**
     * Validate the put-dataset request against the supplied project metadata and build the domain
     * {@link Dataset}. Callable from the coordinator (pre-check, possibly against stale state) and
     * from inside the CAS task (authoritative, against master's current state). Throws cleanly on
     * missing parent, unknown validator, or validation failure.
     */
    Dataset validatePutDataset(ProjectMetadata projectMetadata, PutDatasetAction.Request request) {
        final DataSource parent = DataSourceMetadata.get(projectMetadata).get(request.dataSource());
        if (parent == null) {
            throw new ResourceNotFoundException("data source [{}] not found", request.dataSource());
        }
        final IndexAbstraction existing = projectMetadata.getIndicesLookup().get(request.name());
        if (existing != null && existing.getType() != IndexAbstraction.Type.DATASET) {
            throw new ResourceAlreadyExistsException(
                "dataset [{}] cannot be created, an existing {} with that name is present",
                request.name(),
                existing.getType().getDisplayName()
            );
        }
        final DataSourceValidator validator = validatorsByType.get(parent.type());
        if (validator == null) {
            throw new IllegalStateException("no validator registered for data source type [" + parent.type() + "]");
        }
        final Map<String, Object> validatedSettings = validator.validateDataset(
            parent.settings().asMap(),
            request.resource(),
            request.rawSettings()
        );
        // Reject dataset settings that shadow a parent secret-keyed setting. Check both pre- and
        // post-validator keys: a validator that strips the key before returning would otherwise mask
        // the shadow attempt at the wire boundary.
        Set<String> shadowCandidates = new HashSet<>(validatedSettings.keySet());
        if (request.rawSettings() != null) {
            shadowCandidates.addAll(request.rawSettings().keySet());
        }
        for (String key : shadowCandidates) {
            DataSourceSetting parentSetting = parent.settings().get(key);
            if (parentSetting != null && parentSetting.secret()) {
                ValidationException ex = new ValidationException();
                ex.addValidationError("dataset setting [" + key + "] shadows a secret data-source setting; remove from dataset settings");
                throw ex;
            }
        }
        // Shape-only validation of the declared mapping (no file I/O): declarable types, rename name collisions,
        // and the _id.path reference. A `path` column rename is honored by all formats (translation is centralized at
        // the reader boundary).
        DeclaredSchemaValidator.validate(request.mapping());
        return new Dataset(
            request.name(),
            new DataSourceReference(request.dataSource()),
            request.resource(),
            request.description(),
            validatedSettings,
            request.mapping()
        );
    }

    /**
     * Create or replace a dataset. Validation is expected to have run on the coordinator (via
     * {@link #validatePutDataset}); the task re-validates under CAS to guard against the parent
     * being delete-recreated between coord-validate and task-execute.
     */
    public void putDataset(ProjectId projectId, PutDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final Dataset dataset;
        try {
            dataset = validatePutDataset(projectMetadata, request);
        } catch (Exception e) {
            recordRejected(parentType(projectMetadata, request.dataSource()), e);
            listener.onFailure(e);
            return;
        }
        // No-op if identical to the registered dataset — skip the cluster-state update (mirrors ViewService.putView).
        if (dataset.equals(getMetadata(projectMetadata).get(dataset.name()))) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        logger.debug("submitting put dataset [{}] with parent [{}]", request.name(), request.dataSource());
        final AtomicReference<String> pendingOp = new AtomicReference<>();
        final String type = parentType(projectMetadata, request.dataSource());
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, recordingListener(listener, type, pendingOp)) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return executePutDatasetTaskBody(currentState, projectId, request, pendingOp);
            }
        };
        taskQueue.submitTask("update-esql-dataset-metadata-[" + request.name() + "]", task, task.timeout());
    }

    /** Records a pre-submit or transport pre-check refusal. Used by PUT transport {@code doExecute}. */
    public void recordRejected(String type, Exception e) {
        ConfigChangeTelemetry.recordRejected(metrics, ConfigChangeTelemetry.KIND_DATASET, type, e);
    }

    /** Like {@link #recordRejected(String, Exception)}, resolving type from the parent data source. */
    public void recordRejected(ProjectMetadata project, String dataSourceName, Exception e) {
        recordRejected(parentType(project, dataSourceName), e);
    }

    private static String parentType(ProjectMetadata project, String dataSourceName) {
        DataSource parent = DataSourceMetadata.get(project).get(dataSourceName);
        return parent == null ? null : parent.type();
    }

    // Runs inside the CAS task (see execute() above); package-private for test visibility.
    ClusterState executePutDatasetTaskBody(ClusterState currentState, ProjectId projectId, PutDatasetAction.Request request) {
        return executePutDatasetTaskBody(currentState, projectId, request, null);
    }

    private ClusterState executePutDatasetTaskBody(
        ClusterState currentState,
        ProjectId projectId,
        PutDatasetAction.Request request,
        AtomicReference<String> pendingOp
    ) {
        final ProjectMetadata project = currentState.metadata().getProject(projectId);
        final Dataset dataset = validatePutDataset(project, request);
        final DatasetMetadata metadata = getMetadata(project);
        final Dataset current = metadata.get(dataset.name());
        if (dataset.equals(current)) {
            // Became a no-op between the coordinator check and the task — nothing to write.
            return currentState;
        }
        if (current == null && metadata.datasets().size() >= maxDatasetsCount) {
            logger.warn("rejected put for dataset [{}]: maximum count [{}] reached", dataset.name(), maxDatasetsCount);
            throw new MaxDatasetsCountException(maxDatasetsCount);
        }
        final Map<String, Dataset> updated = new HashMap<>(metadata.datasets());
        updated.put(dataset.name(), dataset);
        if (pendingOp != null) {
            pendingOp.set(current == null ? ConfigChangeTelemetry.OP_CREATED : ConfigChangeTelemetry.OP_UPDATED);
        }
        return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
    }

    /** Delete datasets by name. Fails with 404 if any name doesn't exist. */
    public void deleteDatasets(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Collection<String> names,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DatasetMetadata metadata = getMetadata(projectMetadata);
        final Optional<String> notFound = names.stream().filter(n -> metadata.get(n) == null).findAny();
        if (notFound.isPresent()) {
            ResourceNotFoundException e = new ResourceNotFoundException("dataset [{}] not found", notFound.get());
            recordRejected(null, e);
            listener.onFailure(e);
            return;
        }
        logger.debug("submitting delete datasets {}", names);
        final AtomicReference<List<String>> removedTypes = new AtomicReference<>();
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(
            masterNodeTimeout,
            ackTimeout,
            deleteRecordingListener(listener, removedTypes)
        ) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DatasetMetadata current = getMetadata(project);
                if (names.stream().allMatch(n -> current.get(n) == null)) {
                    // Idempotent: all targets already gone (e.g. concurrent delete) -> no-op, like ViewService.deleteViews.
                    return currentState;
                }
                final Map<String, Dataset> updated = new HashMap<>(current.datasets());
                List<String> typesRemoved = new ArrayList<>();
                for (String name : names) {
                    Dataset existing = current.get(name);
                    if (existing != null) {
                        typesRemoved.add(parentType(project, existing.dataSource().getName()));
                    }
                    updated.remove(name);
                }
                removedTypes.set(typesRemoved);
                return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
            }
        };
        taskQueue.submitTask("delete-esql-dataset-metadata-" + names, task, task.timeout());
    }

    private ActionListener<AcknowledgedResponse> recordingListener(
        ActionListener<AcknowledgedResponse> delegate,
        String type,
        AtomicReference<String> pendingOp
    ) {
        return ActionListener.wrap(r -> {
            String op = pendingOp.get();
            if (op != null) {
                metrics.recordConfigChange(ConfigChangeTelemetry.KIND_DATASET, op, ConfigChangeTelemetry.typeToken(type), null);
            }
            delegate.onResponse(r);
        }, e -> {
            recordRejected(type, e);
            delegate.onFailure(e);
        });
    }

    private ActionListener<AcknowledgedResponse> deleteRecordingListener(
        ActionListener<AcknowledgedResponse> delegate,
        AtomicReference<List<String>> removedTypes
    ) {
        return ActionListener.wrap(r -> {
            List<String> types = removedTypes.get();
            if (types != null) {
                for (String type : types) {
                    metrics.recordConfigChange(
                        ConfigChangeTelemetry.KIND_DATASET,
                        ConfigChangeTelemetry.OP_DELETED,
                        ConfigChangeTelemetry.typeToken(type),
                        null
                    );
                }
            }
            delegate.onResponse(r);
        }, e -> {
            recordRejected(null, e);
            delegate.onFailure(e);
        });
    }

}
