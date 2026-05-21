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
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
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
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
            parent.settings(),
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
        return new Dataset(
            request.name(),
            new DataSourceReference(request.dataSource()),
            request.resource(),
            request.description(),
            validatedSettings
        );
    }

    /**
     * Create or replace a dataset. Validation is expected to have run on the coordinator (via
     * {@link #validatePutDataset}); the task re-validates under CAS to guard against the parent
     * being delete-recreated between coord-validate and task-execute.
     */
    public void putDataset(ProjectId projectId, PutDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        logger.debug("submitting put dataset [{}] with parent [{}]", request.name(), request.dataSource());
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return executePutDatasetTaskBody(currentState, projectId, request);
            }
        };
        taskQueue.submitTask("update-esql-dataset-metadata-[" + request.name() + "]", task, task.timeout());
    }

    // Runs inside the CAS task (see execute() above); package-private for test visibility.
    ClusterState executePutDatasetTaskBody(ClusterState currentState, ProjectId projectId, PutDatasetAction.Request request) {
        final ProjectMetadata project = currentState.metadata().getProject(projectId);
        final Dataset dataset = validatePutDataset(project, request);
        final DatasetMetadata metadata = getMetadata(project);
        final Dataset current = metadata.get(dataset.name());
        if (current == null && metadata.datasets().size() >= maxDatasetsCount) {
            logger.warn("rejected put for dataset [{}]: maximum count [{}] reached", dataset.name(), maxDatasetsCount);
            throw new IllegalArgumentException("cannot add dataset, the maximum number of datasets is reached: " + maxDatasetsCount);
        }
        final Map<String, Dataset> updated = new HashMap<>(metadata.datasets());
        updated.put(dataset.name(), dataset);
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
            listener.onFailure(new ResourceNotFoundException("dataset [{}] not found", notFound.get()));
            return;
        }
        logger.debug("submitting delete datasets {}", names);
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(masterNodeTimeout, ackTimeout, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DatasetMetadata current = getMetadata(project);
                final Map<String, Dataset> updated = new HashMap<>(current.datasets());
                for (String name : names) {
                    if (updated.containsKey(name) == false) {
                        throw new ResourceNotFoundException("dataset [{}] not found", name);
                    }
                    updated.remove(name);
                }
                return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(project).datasets(updated)).build();
            }
        };
        taskQueue.submitTask("delete-esql-dataset-metadata-" + names, task, task.timeout());
    }

}
