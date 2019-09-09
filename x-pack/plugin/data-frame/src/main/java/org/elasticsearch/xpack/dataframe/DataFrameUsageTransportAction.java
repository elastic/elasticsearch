/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.dataframe.DataFrameFeatureSetUsage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DataFrameUsageTransportAction extends XPackUsageFeatureTransportAction {

    private static final Logger logger = LogManager.getLogger(DataFrameUsageTransportAction.class);

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;

    @Inject
    public DataFrameUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         Settings settings, XPackLicenseState licenseState, Client client) {
        super(XPackUsageFeatureAction.DATA_FRAME.name(), transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver);
        this.enabled = XPackSettings.DATA_FRAME_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean available = licenseState.isDataFrameAllowed();
        if (enabled == false) {
            var usage = new DataFrameFeatureSetUsage(available, enabled, Collections.emptyMap(), new DataFrameIndexerTransformStats());
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        PersistentTasksCustomMetaData taskMetadata = PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(state);
        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> dataFrameTasks = taskMetadata == null ?
            Collections.emptyList() :
            taskMetadata.findTasks(DataFrameTransform.NAME, (t) -> true);
        final int taskCount = dataFrameTasks.size();
        final Map<String, Long> transformsCountByState = new HashMap<>();
        for(PersistentTasksCustomMetaData.PersistentTask<?> dataFrameTask : dataFrameTasks) {
            DataFrameTransformState transformState = (DataFrameTransformState)dataFrameTask.getState();
            transformsCountByState.merge(transformState.getTaskState().value(), 1L, Long::sum);
        }

        ActionListener<DataFrameIndexerTransformStats> totalStatsListener = ActionListener.wrap(
            statSummations -> {
                var usage = new DataFrameFeatureSetUsage(available, enabled, transformsCountByState, statSummations);
                listener.onResponse(new XPackUsageFeatureResponse(usage));
            },
            listener::onFailure
        );

        ActionListener<SearchResponse> totalTransformCountListener = ActionListener.wrap(
            transformCountSuccess -> {
                if (transformCountSuccess.getShardFailures().length > 0) {
                    logger.error("total transform count search returned shard failures: {}",
                        Arrays.toString(transformCountSuccess.getShardFailures()));
                }
                long totalTransforms = transformCountSuccess.getHits().getTotalHits().value;
                if (totalTransforms == 0) {
                    var usage = new DataFrameFeatureSetUsage(available, enabled, transformsCountByState,
                        new DataFrameIndexerTransformStats());
                    listener.onResponse(new XPackUsageFeatureResponse(usage));
                    return;
                }
                transformsCountByState.merge(DataFrameTransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
                DataFrameInfoTransportAction.getStatisticSummations(client, totalStatsListener);
            },
            transformCountFailure -> {
                if (transformCountFailure instanceof ResourceNotFoundException) {
                    DataFrameInfoTransportAction.getStatisticSummations(client, totalStatsListener);
                } else {
                    listener.onFailure(transformCountFailure);
                }
            }
        );

        SearchRequest totalTransformCount = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME_PATTERN)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformConfig.NAME))))
            .request();

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            totalTransformCount,
            totalTransformCountListener,
            client::search);
    }
}
