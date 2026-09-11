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
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;
import org.elasticsearch.xpack.esql.datasources.ConfigChangeTelemetry;
import org.elasticsearch.xpack.esql.datasources.MaxDataSourcesCountException;
import org.elasticsearch.xpack.esql.datasources.UnknownDataSourceTypeException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
    private final EncryptionService encryptionService;
    private final ExternalSourceMetrics metrics;

    private volatile int maxDataSourcesCount;

    public DataSourceService(
        ClusterService clusterService,
        Map<String, DataSourceValidator> validatorsByType,
        EncryptionService encryptionService
    ) {
        this(clusterService, validatorsByType, encryptionService, ExternalSourceMetrics.NOOP);
    }

    public DataSourceService(
        ClusterService clusterService,
        Map<String, DataSourceValidator> validatorsByType,
        EncryptionService encryptionService,
        ExternalSourceMetrics metrics
    ) {
        this.clusterService = clusterService;
        this.validatorsByType = Map.copyOf(validatorsByType);
        this.encryptionService = Objects.requireNonNull(encryptionService, "encryptionService");
        this.metrics = metrics == null ? ExternalSourceMetrics.NOOP : metrics;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-data-source-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        // The ceiling is watched while the setting exists, which is while the federation feature is registered (see
        // Federation#settings). Where the feature is unregistered the ceiling is the setting's default and cannot be
        // changed, which no request observes because the CRUD REST routes are not registered either.
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(MAX_DATA_SOURCES_COUNT_SETTING, v -> this.maxDataSourcesCount = v);
    }

    protected DataSourceMetadata getMetadata(ProjectMetadata projectMetadata) {
        return DataSourceMetadata.get(projectMetadata);
    }

    /** Validator registered for {@code type}, or {@code null} when the type is unknown. */
    @Nullable
    public DataSourceValidator validatorFor(String type) {
        return validatorsByType.get(type);
    }

    /**
     * Validate the put-data-source request and build the domain {@link DataSource}.
     */
    public DataSource validatePutDataSource(ProjectMetadata project, PutDataSourceAction.Request request) {
        DataSourceValidator validator = validatorsByType.get(request.type());
        if (validator == null) {
            throw new UnknownDataSourceTypeException(request.type());
        }
        final DataSource current = getMetadata(project).get(request.name());
        Set<String> existingSecretKeys = new HashSet<>();
        if (current != null && current.type().equals(request.type())) {
            for (var entry : current.settings()) {
                if (isUntouchedSecret(entry.getValue(), entry.getKey(), request.rawSettings())) {
                    existingSecretKeys.add(entry.getKey());
                }
            }
        }
        final Map<String, DataSourceSetting> validated = validator.validateDatasource(request.rawSettings(), existingSecretKeys);
        return new DataSource(request.name(), request.type(), request.description(), validated);
    }

    /**
     * Create or replace a data source. A newly-supplied secret is encrypted master-side
     * ({@link #applyEncryption}) off the CAS task thread, since that's expensive and would otherwise block
     * the master on every concurrent PUT. A secret omitted from the request is instead carried forward from
     * the current entry inside the CAS task (via {@link #mergeCarriedForwardSecrets}), where {@code current}
     * is read fresh against authoritative state and carrying the secret forward needs no encryption. The task
     * also re-validates against that same fresh state, so a concurrent change to a secret this request relies
     * on carrying forward fails the PUT instead of silently persisting an incomplete data source. Every other
     * field is a full replace, matching the pre-existing PUT semantics.
     */
    public void putDataSource(ProjectId projectId, PutDataSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final DataSource validated;
        final DataSourceSettings encryptedNew;
        try {
            final ProjectMetadata projectSnapshot = clusterService.state().metadata().getProject(projectId);
            validated = validatePutDataSource(projectSnapshot, request);
            encryptedNew = applyEncryption(validated.name(), validated.settings());
        } catch (Exception e) {
            recordRejected(request.type(), e);
            listener.onFailure(e);
            return;
        }
        logger.debug("submitting put data source [{}] of type [{}]", validated.name(), validated.type());
        final AtomicReference<String> pendingOp = new AtomicReference<>();
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(
            request,
            recordingListener(listener, request.type(), pendingOp)
        ) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata metadata = getMetadata(project);
                final DataSource current = metadata.get(validated.name());
                if (current == null && metadata.dataSources().size() >= maxDataSourcesCount) {
                    logger.warn("rejected put for data source [{}]: maximum count [{}] reached", validated.name(), maxDataSourcesCount);
                    throw new MaxDataSourcesCountException(maxDataSourcesCount);
                }
                // Re-validate here, against the state just read, not the pre-encryption snapshot above: a
                // concurrent operation could have cleared or removed a secret this request relies on carrying
                // forward between that snapshot and this task running. Cheap (no I/O); throwing here fails the
                // whole PUT instead of silently persisting a data source with incomplete credentials.
                validatePutDataSource(project, request);
                final DataSourceSettings merged = mergeCarriedForwardSecrets(current, validated.type(), encryptedNew, request);
                final DataSource encrypted = new DataSource(validated.name(), validated.type(), validated.description(), merged);
                final Map<String, DataSource> updated = new HashMap<>(metadata.dataSources());
                updated.put(encrypted.name(), encrypted);
                pendingOp.set(current == null ? ConfigChangeTelemetry.OP_CREATED : ConfigChangeTelemetry.OP_UPDATED);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("update-esql-data-source-metadata-[" + request.name() + "]", task, task.timeout());
    }

    /** Records a pre-submit or transport pre-check refusal. Used by PUT transport {@code doExecute}. */
    public void recordRejected(String type, Exception e) {
        ConfigChangeTelemetry.recordRejected(metrics, ConfigChangeTelemetry.KIND_DATASOURCE, type, e);
    }

    /**
     * True iff {@code setting} is a secret the request leaves untouched, so it should carry forward from the
     * existing entry rather than being wiped.
     */
    static boolean isUntouchedSecret(DataSourceSetting setting, String key, Map<String, Object> rawSettings) {
        return setting.secret() && setting.rawValue() != null && rawSettings.containsKey(key) == false;
    }

    /**
     * Adds every untouched secret from {@code current} (same type, not already present in {@code encryptedNew})
     * to the settings being stored, so it survives a PUT that only touches other fields.
     */
    private static DataSourceSettings mergeCarriedForwardSecrets(
        @Nullable DataSource current,
        String requestType,
        DataSourceSettings encryptedNew,
        PutDataSourceAction.Request request
    ) {
        if (current == null || current.type().equals(requestType) == false) {
            return encryptedNew;
        }
        Map<String, DataSourceSetting> merged = new HashMap<>(encryptedNew.asMap());
        for (var entry : current.settings()) {
            String key = entry.getKey();
            if (merged.containsKey(key)) {
                continue;
            }
            if (isUntouchedSecret(entry.getValue(), key, request.rawSettings())) {
                merged.put(key, entry.getValue());
            }
        }
        return new DataSourceSettings(merged);
    }

    /**
     * Replace every non-null secret with an {@link EncryptedData} carrier.
     *
     * <p>When encryption is permanently unavailable ({@link EncryptionServiceUnavailableException}) and
     * {@code isEncryptionRequired()} is {@code true} (the default), the call throws a {@code 503} with an actionable message.
     * When {@code isEncryptionRequired()} is {@code false}, secrets are stored unencrypted with a {@code WARN} log — this is an
     * explicit operator opt-out via {@code cluster.state.encryption.required: false}.
     *
     * <p>Transient unavailability ({@link EncryptionKeyNotYetAvailableException}, e.g. cluster still recovering) always throws
     * regardless of {@code isEncryptionRequired()}, since the key will become available and the caller should retry.
     *
     * <p>Settings with no secrets, and already-encrypted carriers, pass through unchanged.
     */
    DataSourceSettings applyEncryption(String dataSourceName, DataSourceSettings settings) {
        try {
            return encryptSettings(settings);
        } catch (EncryptionKeyNotYetAvailableException e) {
            throw new ElasticsearchStatusException(
                "cannot store secrets for data source [" + dataSourceName + "]: " + e.getMessage() + " Retry once the cluster is ready.",
                RestStatus.SERVICE_UNAVAILABLE,
                e
            );
        } catch (EncryptionServiceUnavailableException e) {
            if (encryptionService.isEncryptionRequired()) {
                throw new ElasticsearchStatusException(
                    "cannot store secrets for data source [" + dataSourceName + "]: " + e.getMessage(),
                    RestStatus.SERVICE_UNAVAILABLE,
                    e
                );
            }
            logger.warn(
                "storing secrets for data source [{}] without encryption: {}. "
                    + "Set cluster.state.encryption.required: true (the default) to enforce encryption.",
                dataSourceName,
                e.getMessage()
            );
            return settings;
        }
    }

    private DataSourceSettings encryptSettings(DataSourceSettings settings) {
        Map<String, DataSourceSetting> result = new HashMap<>(settings.size());
        for (var entry : settings) {
            String key = entry.getKey();
            DataSourceSetting setting = entry.getValue();
            // Skip null-valued secrets (nothing to protect) and already-encrypted carriers (no double-encryption).
            if (setting.secret() && setting.rawValue() != null && setting.isEncrypted() == false) {
                result.put(key, encryptSecret(setting.rawValue()));
            } else {
                result.put(key, setting);
            }
        }
        return new DataSourceSettings(result);
    }

    /**
     * Serialize the value with {@code writeGenericValue} (so non-String secrets round-trip) and encrypt it;
     * the plaintext buffer is zeroed after. The source value object outlives this call until the CAS task
     * completes — narrowing that is Phase 2.
     */
    private DataSourceSetting encryptSecret(Object value) {
        byte[] plaintext = serializeValue(value);
        try {
            return new DataSourceSetting(encryptionService.encrypt(plaintext), true);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    private static byte[] serializeValue(Object value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(value);
            return BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new ElasticsearchStatusException(
                "failed to serialize secret data source setting value for encryption",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
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
            ResourceNotFoundException e = new ResourceNotFoundException("data source [{}] not found", notFound.get());
            recordRejected(null, e);
            listener.onFailure(e);
            return;
        }
        logger.debug("submitting delete data sources {}", names);
        final AtomicReference<List<String>> removedTypes = new AtomicReference<>();
        final AtomicReference<String> failureType = new AtomicReference<>();
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(
            masterNodeTimeout,
            ackTimeout,
            deleteRecordingListener(listener, removedTypes, failureType)
        ) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final DataSourceMetadata current = getMetadata(project);
                final Map<String, DataSource> updated = new HashMap<>(current.dataSources());
                List<String> typesRemoved = new ArrayList<>();
                for (String name : names) {
                    DataSource existing = updated.get(name);
                    if (existing == null) {
                        // Do not keep a prior name's type: this refusal is for a missing name.
                        failureType.set(null);
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
                        failureType.set(existing.type());
                        throw new ElasticsearchStatusException(
                            "cannot delete data source [" + name + "]: referenced by datasets " + dependents,
                            RestStatus.CONFLICT
                        );
                    }
                    updated.remove(name);
                    typesRemoved.add(existing.type());
                }
                removedTypes.set(typesRemoved);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(project).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(updated))
                    )
                    .build();
            }
        };
        taskQueue.submitTask("delete-esql-data-source-metadata-" + names, task, task.timeout());
    }

    private ActionListener<AcknowledgedResponse> recordingListener(
        ActionListener<AcknowledgedResponse> delegate,
        String type,
        AtomicReference<String> pendingOp
    ) {
        return ActionListener.wrap(r -> {
            String op = pendingOp.get();
            if (op != null) {
                metrics.recordConfigChange(ConfigChangeTelemetry.KIND_DATASOURCE, op, ConfigChangeTelemetry.typeToken(type), null);
            }
            delegate.onResponse(r);
        }, e -> {
            recordRejected(type, e);
            delegate.onFailure(e);
        });
    }

    private ActionListener<AcknowledgedResponse> deleteRecordingListener(
        ActionListener<AcknowledgedResponse> delegate,
        AtomicReference<List<String>> removedTypes,
        AtomicReference<String> failureType
    ) {
        return ActionListener.wrap(r -> {
            List<String> types = removedTypes.get();
            if (types != null) {
                for (String type : types) {
                    metrics.recordConfigChange(
                        ConfigChangeTelemetry.KIND_DATASOURCE,
                        ConfigChangeTelemetry.OP_DELETED,
                        ConfigChangeTelemetry.typeToken(type),
                        null
                    );
                }
            }
            delegate.onResponse(r);
        }, e -> {
            recordRejected(failureType.get(), e);
            delegate.onFailure(e);
        });
    }

}
