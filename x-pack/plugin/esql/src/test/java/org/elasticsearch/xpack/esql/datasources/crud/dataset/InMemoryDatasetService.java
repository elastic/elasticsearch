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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** In-memory {@link DatasetService} stand-in for unit tests — inlines the CAS task body synchronously. */
public class InMemoryDatasetService extends DatasetService implements Closeable {

    private static final Set<Setting<?>> ALL_SETTINGS;
    static {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(MAX_DATASETS_COUNT_SETTING);
        settings.add(DataSourceServiceMaxCountSettingRef.SETTING);
        ALL_SETTINGS = settings;
    }

    private final ThreadPool threadPool;
    private final Map<String, DataSourceValidator> validatorsByType;

    /** Create an in-memory service with its own {@link TestThreadPool} + {@link ClusterService}. */
    public static InMemoryDatasetService make(Map<String, DataSourceValidator> validatorsByType) {
        return make(Settings.EMPTY, validatorsByType);
    }

    public static InMemoryDatasetService make(Settings settings, Map<String, DataSourceValidator> validatorsByType) {
        TestThreadPool threadPool = new TestThreadPool("in-memory-dataset-service");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(settings, ALL_SETTINGS));
        return new InMemoryDatasetService(clusterService, threadPool, validatorsByType);
    }

    /** Share an existing {@link ClusterService} — used for joint tests with a sibling data-source service. */
    public static InMemoryDatasetService sharing(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType) {
        return new InMemoryDatasetService(clusterService, null, validatorsByType);
    }

    private InMemoryDatasetService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Map<String, DataSourceValidator> validatorsByType
    ) {
        super(clusterService, validatorsByType);
        this.threadPool = threadPool;
        this.validatorsByType = Map.copyOf(validatorsByType);
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    @Override
    public void putDataset(ProjectId projectId, PutDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            // 1. Parent data-source lookup from current cluster state.
            final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
            final DataSource parent = DataSourceMetadata.get(projectMetadata).get(request.dataSource());
            if (parent == null) {
                listener.onFailure(new ResourceNotFoundException("data source [{}] not found", request.dataSource()));
                return;
            }
            // 2. Validator dispatch via the parent's type.
            final DataSourceValidator validator = validatorsByType.get(parent.type());
            if (validator == null) {
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
            DatasetMetadata datasetMetadata = DatasetMetadata.get(projectMetadata);
            final Dataset existing = datasetMetadata.get(dataset.name());
            if (dataset.equals(existing)) {
                listener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            // 4. Max-count check.
            int max = clusterService.getClusterSettings().get(MAX_DATASETS_COUNT_SETTING);
            if (existing == null && datasetMetadata.datasets().size() >= max) {
                listener.onFailure(new IllegalArgumentException("cannot add dataset, the maximum number of datasets is reached: " + max));
                return;
            }
            // 5. Build updated project (name-collision check runs in Builder.build() via ensureNoNameCollisions).
            Map<String, Dataset> updated = new HashMap<>(datasetMetadata.datasets());
            updated.put(dataset.name(), dataset);
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectMetadata).datasets(updated);
            ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build());
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void deleteDataset(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String name,
        ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            final ProjectMetadata projectMetadata = clusterService.state().metadata().getProject(projectId);
            final DatasetMetadata datasetMetadata = DatasetMetadata.get(projectMetadata);
            if (datasetMetadata.get(name) == null) {
                listener.onFailure(new ResourceNotFoundException("dataset [{}] not found", name));
                return;
            }
            Map<String, Dataset> updated = new HashMap<>(datasetMetadata.datasets());
            updated.remove(name);
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectMetadata).datasets(updated);
            ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build());
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void close() {
        if (threadPool != null) {
            clusterService.close();
            threadPool.shutdownNow();
        }
    }

    /** Forward declaration — registered in ALL_SETTINGS so a shared ClusterService sees both sides' settings. */
    private static final class DataSourceServiceMaxCountSettingRef {
        static final Setting<Integer> SETTING =
            org.elasticsearch.xpack.esql.datasources.crud.datasource.DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING;
    }
}
