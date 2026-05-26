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
import org.elasticsearch.cluster.metadata.DataSourceSettings;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
     * coordinator (pre-check) and from inside the CAS task (authoritative re-validate).
     */
    public DataSource validatePutDataSource(PutDataSourceAction.Request request) {
        DataSourceValidator validator = validatorsByType.get(request.type());
        if (validator == null) {
            throw new IllegalArgumentException("unknown data source type [" + request.type() + "]");
        }
        final Map<String, DataSourceSetting> validated = validator.validateDatasource(request.rawSettings());
        return new DataSource(request.name(), request.type(), request.description(), validated);
    }

    /**
     * Create or replace a data source. When {@code encryptionService} is bound, every secret in the
     * request is encrypted master-side; when it isn't, secrets are stored as plaintext and a
     * {@code WARN} naming the data source is logged so the operator sees exactly which credentials
     * hit the disk in clear text. The consumer-side decryption step still refuses to hand the
     * connector an encrypted blob without a key (the only asymmetric mode is "unbound at PUT, then
     * plaintext at FROM" — what you'd expect on a dev / no-security cluster).
     */
    public void putDataSource(
        ProjectId projectId,
        PutDataSourceAction.Request request,
        @Nullable EncryptionService encryptionService,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Validate and encrypt off the cluster-state update thread. Encryption is expensive and must not
        // run inside the CAS task body — the master would block on every concurrent PUT otherwise.
        final DataSource validated = validatePutDataSource(request);
        final DataSourceSettings stored = applyEncryption(validated.name(), validated.settings(), encryptionService);
        final DataSource encrypted = new DataSource(validated.name(), validated.type(), validated.description(), stored);
        logger.debug("submitting put data source [{}] of type [{}]", encrypted.name(), encrypted.type());
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata metadata = getMetadata(project);
                final DataSource current = metadata.get(encrypted.name());
                if (current == null && metadata.dataSources().size() >= maxDataSourcesCount) {
                    logger.warn("rejected put for data source [{}]: maximum count [{}] reached", encrypted.name(), maxDataSourcesCount);
                    throw new IllegalArgumentException(
                        "cannot add data source, the maximum number of data sources is reached: " + maxDataSourcesCount
                    );
                }
                final Map<String, DataSource> updated = new HashMap<>(metadata.dataSources());
                updated.put(encrypted.name(), encrypted);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("update-esql-data-source-metadata-[" + request.name() + "]", task, task.timeout());
    }

    /**
     * Apply master-side encryption to a data source's settings. When {@code encryptionService} is null
     * (security plugin absent or PEK feature flag off), the settings pass through unchanged and a WARN
     * naming the data source is logged if any of them carries a real secret.
     */
    private static DataSourceSettings applyEncryption(
        String dataSourceName,
        DataSourceSettings settings,
        @Nullable EncryptionService encryptionService
    ) {
        if (encryptionService == null) {
            if (settings.hasSecrets()) {
                logger.warn(
                    "credentials for data source [{}] are stored as plaintext because no encryption service is available",
                    dataSourceName
                );
            }
            return settings;
        }
        Map<String, DataSourceSetting> result = new HashMap<>(settings.size());
        for (var entry : settings) {
            String key = entry.getKey();
            DataSourceSetting setting = entry.getValue();
            if (setting.secret() && setting.rawValue() instanceof String plaintext) {
                result.put(key, encryptSecret(key, plaintext, encryptionService));
            } else {
                result.put(key, setting);
            }
        }
        return new DataSourceSettings(result);
    }

    /**
     * Encrypt one plaintext into a freshly-built ciphertext-carrying setting. The intermediate
     * UTF-8 byte buffer is zeroed in {@code finally}; the original plaintext String reference still
     * lives in the caller's settings until the cluster-state task completes (narrowing that
     * lifetime is Phase 2).
     */
    private static DataSourceSetting encryptSecret(String key, String plaintext, EncryptionService encryptionService) {
        byte[] bytes = plaintext.getBytes(StandardCharsets.UTF_8);
        try {
            EncryptedData encrypted = encryptionService.encrypt(bytes);
            BytesStreamOutput out = new BytesStreamOutput();
            try {
                encrypted.writeTo(out);
            } catch (IOException e) {
                throw new ElasticsearchStatusException(
                    "failed to serialize encrypted value for setting [" + key + "]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                );
            }
            byte[] blob = BytesReference.toBytes(out.bytes());
            return new DataSourceSetting(blob, true, DataSourceSetting.EncryptionFormat.V1);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
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
