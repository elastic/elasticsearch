/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** In-memory {@link DataSourceService} stand-in for unit tests — inlines the CAS task body synchronously. */
public class InMemoryDataSourceService extends DataSourceService implements Closeable {

    private static final Set<Setting<?>> ALL_SETTINGS;
    static {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(MAX_DATA_SOURCES_COUNT_SETTING);
        settings.add(DatasetServiceMaxCountSettingRef.SETTING);
        ALL_SETTINGS = settings;
    }

    private final ThreadPool threadPool;
    private final Map<String, DataSourceValidator> validatorsByType;
    private DataSourceMetadata dataSourceMetadata = DataSourceMetadata.EMPTY;

    /**
     * Create an in-memory service wired to its own {@link TestThreadPool} and {@link ClusterService}.
     * Test owns the returned service and must {@link #close()} it to release the threadpool.
     */
    public static InMemoryDataSourceService make(Map<String, DataSourceValidator> validatorsByType) {
        return make(Settings.EMPTY, validatorsByType);
    }

    public static InMemoryDataSourceService make(Settings settings, Map<String, DataSourceValidator> validatorsByType) {
        TestThreadPool threadPool = new TestThreadPool("in-memory-data-source-service");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(settings, ALL_SETTINGS));
        return new InMemoryDataSourceService(clusterService, threadPool, validatorsByType);
    }

    /** Create an in-memory service sharing an existing {@link ClusterService} — used for joint tests. */
    public static InMemoryDataSourceService sharing(ClusterService clusterService, Map<String, DataSourceValidator> validatorsByType) {
        return new InMemoryDataSourceService(clusterService, null, validatorsByType);
    }

    private InMemoryDataSourceService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Map<String, DataSourceValidator> validatorsByType
    ) {
        super(clusterService, validatorsByType);
        this.threadPool = threadPool;
        this.validatorsByType = Map.copyOf(validatorsByType);
    }

    /** Test-only accessor so a companion {@code InMemoryDatasetService} can share state. */
    public ClusterService clusterService() {
        return clusterService;
    }

    @Override
    protected DataSourceMetadata getMetadata(ProjectMetadata projectMetadata) {
        return dataSourceMetadata;
    }

    @Override
    public void putDataSource(ProjectId projectId, PutDataSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            DataSourceValidator validator = validatorsByType.get(request.type());
            if (validator == null) {
                listener.onFailure(new IllegalArgumentException("unknown data source type [" + request.type() + "]"));
                return;
            }
            final Map<String, org.elasticsearch.cluster.metadata.DataSourceSetting> validated;
            try {
                validated = validator.validateDatasource(request.rawSettings());
            } catch (ValidationException e) {
                listener.onFailure(e);
                return;
            }
            DataSource dataSource = new DataSource(request.name(), request.type(), request.description(), validated);

            DataSource existing = dataSourceMetadata.get(dataSource.name());
            if (dataSource.equals(existing)) {
                listener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            int max = clusterService.getClusterSettings().get(MAX_DATA_SOURCES_COUNT_SETTING);
            if (existing == null && dataSourceMetadata.dataSources().size() >= max) {
                listener.onFailure(
                    new IllegalArgumentException("cannot add data source, the maximum number of data sources is reached: " + max)
                );
                return;
            }
            Map<String, DataSource> updated = new HashMap<>(dataSourceMetadata.dataSources());
            updated.put(dataSource.name(), dataSource);
            dataSourceMetadata = new DataSourceMetadata(updated);
            pushProjectCustom(projectId);
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void deleteDataSource(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String name,
        ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            if (dataSourceMetadata.get(name) == null) {
                listener.onFailure(new ResourceNotFoundException("data source [{}] not found", name));
                return;
            }
            // Referential integrity — look at the companion service's state via cluster state.
            ProjectMetadata project = clusterService.state().metadata().getProject(projectId);
            DatasetMetadata datasets = DatasetMetadata.get(project);
            List<String> dependents = datasets.datasets()
                .values()
                .stream()
                .filter(ds -> name.equals(ds.dataSource().getName()))
                .map(Dataset::name)
                .toList();
            if (dependents.isEmpty() == false) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "cannot delete data source [" + name + "]: referenced by datasets " + dependents,
                        RestStatus.CONFLICT
                    )
                );
                return;
            }
            Map<String, DataSource> updated = new HashMap<>(dataSourceMetadata.dataSources());
            updated.remove(name);
            dataSourceMetadata = new DataSourceMetadata(updated);
            pushProjectCustom(projectId);
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** Push the current {@code dataSourceMetadata} into cluster state, preserving any other existing project customs. */
    private void pushProjectCustom(ProjectId projectId) {
        ProjectMetadata currentProject = clusterService.state().metadata().getProject(projectId);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(currentProject).putCustom(DataSourceMetadata.TYPE, dataSourceMetadata);
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build());
    }

    @Override
    public void close() {
        if (threadPool != null) {
            // We own the ClusterService + ThreadPool; tear them down. In the sharing() factory we don't.
            clusterService.close();
            threadPool.shutdownNow();
        }
    }

    /**
     * Forward declaration of the dataset-side max-count setting — registered in {@code ALL_SETTINGS} so a
     * companion {@code InMemoryDatasetService} wired to the same {@link ClusterService} sees it. The
     * actual setting lives on {@code DatasetService}; this ref keeps that package's test code out of this
     * one while still pulling it into the shared {@link ClusterSettings} at ClusterService construction.
     */
    private static final class DatasetServiceMaxCountSettingRef {
        static final Setting<Integer> SETTING =
            org.elasticsearch.xpack.esql.datasources.crud.dataset.DatasetService.MAX_DATASETS_COUNT_SETTING;
    }
}
