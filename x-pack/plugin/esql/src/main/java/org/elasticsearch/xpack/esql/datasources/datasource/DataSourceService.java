/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Orchestrates create / replace / delete of data sources in cluster state. */
public class DataSourceService {

    private static final Logger logger = LogManager.getLogger(DataSourceService.class);

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

    private final AtomicBoolean encryptionSkippedLogged = new AtomicBoolean(false);

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
     * Validate the put-data-source request and build the domain {@link DataSource}. Callable from the
     * coordinator (pre-check) and from inside the CAS task (authoritative re-validate). The returned
     * {@link DataSource} carries {@code uuid == null}; the master-side put step preserves the existing
     * UUID on update or assigns a fresh one on create / legacy migration.
     */
    public DataSource validatePutDataSource(PutDataSourceAction.Request request) {
        DataSourceValidator validator = validatorsByType.get(request.type());
        if (validator == null) {
            throw new IllegalArgumentException("unknown data source type [" + request.type() + "]");
        }
        final Map<String, org.elasticsearch.cluster.metadata.DataSourceSetting> validated = validator.validateDatasource(
            request.rawSettings()
        );
        return new DataSource(request.name(), request.type(), request.description(), validated, null);
    }

    /**
     * Create or replace a data source. Validates under CAS, encrypts secret settings master-side
     * (only when {@code encryptionService != null} and the cluster advertises
     * {@link EsqlFeatures#DATA_SOURCE_ENCRYPTION_FEATURE}; otherwise plaintext, one-time INFO log).
     * A missing PEK surfaces as 503 via {@code EncryptionKeyNotYetAvailableException.status()}.
     */
    public void putDataSource(
        ProjectId projectId,
        PutDataSourceAction.Request request,
        @Nullable EncryptionService encryptionService,
        FeatureService featureService,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug("submitting put data source [{}] of type [{}]", request.name(), request.type());
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final DataSource validated = validatePutDataSource(request);
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata metadata = getMetadata(project);
                final DataSource current = metadata.get(validated.name());
                if (current == null && metadata.dataSources().size() >= maxDataSourcesCount) {
                    logger.warn("rejected put for data source [{}]: maximum count [{}] reached", validated.name(), maxDataSourcesCount);
                    throw new IllegalArgumentException(
                        "cannot add data source, the maximum number of data sources is reached: " + maxDataSourcesCount
                    );
                }
                final boolean encryptionFeatureSupported = featureService.clusterHasFeature(
                    currentState,
                    EsqlFeatures.DATA_SOURCE_ENCRYPTION_FEATURE
                );
                final Map<String, DataSourceSetting> finalSettings;
                if (encryptionFeatureSupported && encryptionService != null) {
                    finalSettings = encryptSecrets(validated.settings(), encryptionService);
                } else {
                    logEncryptionSkippedOnce(encryptionFeatureSupported, encryptionService);
                    finalSettings = validated.settings();
                }
                // Preserve UUID on update; assign fresh on create or legacy (pre-UUID) migration.
                final String uuid = (current != null && current.uuid() != null) ? current.uuid() : UUIDs.randomBase64UUID();
                final DataSource dataSource = new DataSource(
                    validated.name(),
                    validated.type(),
                    validated.description(),
                    finalSettings,
                    uuid
                );
                final Map<String, DataSource> updated = new HashMap<>(metadata.dataSources());
                updated.put(dataSource.name(), dataSource);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("update-esql-data-source-metadata-[" + request.name() + "]", task, task.timeout());
    }

    private static Map<String, DataSourceSetting> encryptSecrets(
        Map<String, DataSourceSetting> settings,
        EncryptionService encryptionService
    ) {
        Map<String, DataSourceSetting> result = new HashMap<>(settings.size());
        for (var entry : settings.entrySet()) {
            DataSourceSetting setting = entry.getValue();
            if (setting.secret() == false) {
                result.put(entry.getKey(), setting);
                continue;
            }
            Object raw = setting.encryptedSecret();
            if (raw instanceof String plaintext) {
                byte[] bytes = plaintext.getBytes(StandardCharsets.UTF_8);
                try {
                    EncryptedData encrypted = encryptionService.encrypt(bytes);
                    result.put(entry.getKey(), new DataSourceSetting(encrypted, true));
                } finally {
                    Arrays.fill(bytes, (byte) 0);
                }
            } else {
                result.put(entry.getKey(), setting);
            }
        }
        return result;
    }

    private void logEncryptionSkippedOnce(boolean featureSupported, @Nullable EncryptionService encryptionService) {
        if (encryptionSkippedLogged.compareAndSet(false, true) == false) {
            return;
        }
        final String reason;
        if (featureSupported == false) {
            reason = "not every node advertises the ["
                + EsqlFeatures.DATA_SOURCE_ENCRYPTION_FEATURE.id()
                + "] feature yet (rolling upgrade in progress)";
        } else if (encryptionService == null) {
            reason = "EncryptionService is not bound on this node (security plugin disabled or PEK feature flag off)";
        } else {
            reason = "unknown";
        }
        logger.info("data-source secret encryption skipped: {}; secrets stored as plaintext", reason);
    }

    /** Delete data sources by name. Fails with 409 if any dataset references one; 404 if a name doesn't exist. */
    public void deleteDataSources(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Collection<String> names,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
        final DataSourceMetadata metadata = getMetadata(projectMetadata);
        final Optional<String> notFound = names.stream().filter(n -> metadata.get(n) == null).findAny();
        if (notFound.isPresent()) {
            listener.onFailure(new ResourceNotFoundException("data source [{}] not found", notFound.get()));
            return;
        }
        logger.debug("submitting delete data sources {}", names);
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(masterNodeTimeout, ackTimeout, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata current = getMetadata(project);
                final Map<String, DataSource> updated = new HashMap<>(current.dataSources());
                for (String name : names) {
                    if (updated.containsKey(name) == false) {
                        throw new ResourceNotFoundException("data source [{}] not found", name);
                    }
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
                    updated.remove(name);
                }
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("delete-esql-data-source-metadata-" + names, task, task.timeout());
    }

}
