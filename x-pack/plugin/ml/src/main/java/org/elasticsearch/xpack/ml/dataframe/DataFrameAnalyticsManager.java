/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameAnalyticsManager {

    /**
     * Unfortunately, getting the settings of an index include internal settings that should
     * not be set explicitly. There is no way to filter those out. Thus, we have to maintain
     * a list of them and filter them out manually.
     */
    private static final List<String> INTERNAL_SETTINGS = Arrays.asList(
        "index.creation_date",
        "index.provided_name",
        "index.uuid",
        "index.version.created"
    );

    private final ClusterService clusterService;
    private final Client client;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;

    public DataFrameAnalyticsManager(ClusterService clusterService, Client client, DataFrameAnalyticsConfigProvider configProvider,
                                     AnalyticsProcessManager processManager) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
    }

    public void execute(DataFrameAnalyticsTask task) {
        ActionListener<DataFrameAnalyticsConfig> reindexingStateListener = ActionListener.wrap(
            config -> reindexDataframeAndStartAnalysis(task, config),
            e -> task.markAsFailed(e)
        );

        // Update task state to REINDEXING
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                DataFrameAnalyticsTaskState reindexingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.REINDEXING,
                    task.getAllocationId());
                task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                    updatedTask -> reindexingStateListener.onResponse(config),
                    reindexingStateListener::onFailure
                ));
            },
            reindexingStateListener::onFailure
        );

        // Retrieve configuration
        configProvider.get(task.getParams().getId(), configListener);
    }

    private void reindexDataframeAndStartAnalysis(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        // Reindexing is complete; start analytics
        ActionListener<RefreshResponse> refreshListener = ActionListener.wrap(
            refreshResponse -> startAnalytics(task, config),
            task::markAsFailed
        );

        // Refresh to ensure copied index is fully searchable
        ActionListener<BulkByScrollResponse> reindexCompletedListener = ActionListener.wrap(
            bulkResponse -> client.execute(RefreshAction.INSTANCE, new RefreshRequest(config.getDest()), refreshListener),
            e -> task.markAsFailed(e)
        );

        // Reindex
        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(
            createIndexResponse -> {
                ReindexRequest reindexRequest = new ReindexRequest();
                reindexRequest.setSourceIndices(config.getSource());
                reindexRequest.setDestIndex(config.getDest());
                reindexRequest.setScript(new Script("ctx._source." + DataFrameAnalyticsFields.ID + " = ctx._id"));
                client.execute(ReindexAction.INSTANCE, reindexRequest, reindexCompletedListener);
            },
            reindexCompletedListener::onFailure
        );

        createDestinationIndex(config.getSource(), config.getDest(), copyIndexCreatedListener);
    }

    private void startAnalytics(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        // Update state to ANALYZING and start process
        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameAnalyticsTaskState analyzingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.ANALYZING,
                    task.getAllocationId());
                task.updatePersistentTaskState(analyzingState, ActionListener.wrap(
                    updatedTask -> processManager.runJob(config, dataExtractorFactory,
                        error -> {
                            if (error != null) {
                                task.markAsFailed(error);
                            } else {
                                task.markAsCompleted();
                            }
                        }),
                    task::markAsFailed
                ));
            },
            e -> task.markAsFailed(e)
        );

        // TODO This could fail with errors. In that case we get stuck with the copied index.
        // We could delete the index in case of failure or we could try building the factory before reindexing
        // to catch the error early on.
        DataFrameDataExtractorFactory.create(client, Collections.emptyMap(), config.getDest(), dataExtractorFactoryListener);
    }

    private void createDestinationIndex(String sourceIndex, String destinationIndex, ActionListener<CreateIndexResponse> listener) {
        IndexMetaData indexMetaData = clusterService.state().getMetaData().getIndices().get(sourceIndex);
        if (indexMetaData == null) {
            listener.onFailure(new IndexNotFoundException(sourceIndex));
            return;
        }

        Settings.Builder settingsBuilder = Settings.builder().put(indexMetaData.getSettings());
        INTERNAL_SETTINGS.stream().forEach(settingsBuilder::remove);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataFrameAnalyticsFields.ID);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), SortOrder.ASC);

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(destinationIndex, settingsBuilder.build());
        addDestinationIndexMappings(indexMetaData, createIndexRequest);
        client.execute(CreateIndexAction.INSTANCE, createIndexRequest, listener);
    }

    private static void addDestinationIndexMappings(IndexMetaData indexMetaData, CreateIndexRequest createIndexRequest) {
        ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
        Map<String, Object> mappingsAsMap = mappings.valuesIt().next().sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingsAsMap.get("properties");
        Map<String, Object> idCopyMapping = new HashMap<>();
        idCopyMapping.put("type", "keyword");
        properties.put(DataFrameAnalyticsFields.ID, idCopyMapping);

        createIndexRequest.mapping(mappings.keysIt().next(), mappingsAsMap);
    }
}
