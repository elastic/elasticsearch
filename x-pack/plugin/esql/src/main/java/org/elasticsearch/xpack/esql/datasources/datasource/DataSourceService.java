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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

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

    /**
     * CAS-guarded so the "encryption unavailable" warning logs at most once per master-instance
     * lifetime. The flag is per-{@code DataSourceService}, which is per-node; the warning body runs
     * inside the CAS task body, which only executes on the elected master. After a master failover
     * the new master's flag starts at {@code false} and the warning re-fires on its first plaintext
     * PUT — which is the right shape for an audit signal (operators see it every time a fresh master
     * takes over a non-encrypting cluster).
     */
    private final AtomicBoolean plaintextWarningLogged = new AtomicBoolean(false);

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
     * Create or replace a data source. When {@code encryption.isAvailable()}, every secret value is
     * encrypted master-side before being committed to cluster state. When it is not available (e.g. the
     * cluster runs without {@code xpack.security.enabled} or with the PEK feature flag off), secret
     * values are committed as plaintext and a single loud warning is logged at most once per
     * master-instance lifetime (re-fires after a master failover; see {@link #plaintextWarningLogged}).
     * The decrypt seam ({@code DataSourceCredentials.decryptInPlace}) still refuses to hand the
     * connector an encrypted blob without a key, so the only asymmetric mode is "unbound at PUT, then
     * plaintext at FROM" — which is what you'd expect on a dev / no-security cluster.
     */
    public void putDataSource(
        ProjectId projectId,
        PutDataSourceAction.Request request,
        DataSourceEncryption encryption,
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
                final Map<String, DataSourceSetting> stored;
                if (encryption.isAvailable()) {
                    stored = encryption.encrypt(validated.settings());
                } else {
                    if (hasSecret(validated.settings()) && plaintextWarningLogged.compareAndSet(false, true)) {
                        logger.warn(
                            "data-source secrets are being stored as PLAINTEXT in cluster state because "
                                + "no encryption service is bound on this node. To encrypt at rest, enable xpack.security "
                                + "and the primary-encryption-key feature flag. This message is logged at most once per master-instance lifetime."
                        );
                    }
                    stored = validated.settings();
                }
                final DataSource dataSource = new DataSource(validated.name(), validated.type(), validated.description(), stored);
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

    private static boolean hasSecret(Map<String, DataSourceSetting> settings) {
        for (DataSourceSetting s : settings.values()) {
            if (s.secret() && s.rawValue() != null) return true;
        }
        return false;
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
