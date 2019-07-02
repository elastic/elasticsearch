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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameFeatureSetUsage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class DataFrameFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    private static final Logger logger = LogManager.getLogger(DataFrameFeatureSet.class);

    public static final String[] PROVIDED_STATS = new String[] {
        DataFrameIndexerTransformStats.NUM_PAGES.getPreferredName(),
        DataFrameIndexerTransformStats.NUM_INPUT_DOCUMENTS.getPreferredName(),
        DataFrameIndexerTransformStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(),
        DataFrameIndexerTransformStats.NUM_INVOCATIONS.getPreferredName(),
        DataFrameIndexerTransformStats.INDEX_TIME_IN_MS.getPreferredName(),
        DataFrameIndexerTransformStats.SEARCH_TIME_IN_MS.getPreferredName(),
        DataFrameIndexerTransformStats.INDEX_TOTAL.getPreferredName(),
        DataFrameIndexerTransformStats.SEARCH_TOTAL.getPreferredName(),
        DataFrameIndexerTransformStats.INDEX_FAILURES.getPreferredName(),
        DataFrameIndexerTransformStats.SEARCH_FAILURES.getPreferredName(),
    };

    @Inject
    public DataFrameFeatureSet(Settings settings, ClusterService clusterService, Client client, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.DATA_FRAME_ENABLED.get(settings);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.DATA_FRAME;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isDataFrameAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        if (enabled == false) {
            listener.onResponse(new DataFrameFeatureSetUsage(available(),
                enabled(),
                Collections.emptyMap(),
                DataFrameIndexerTransformStats.withDefaultTransformId()));
            return;
        }

        PersistentTasksCustomMetaData taskMetadata = PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterService.state());
        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> dataFrameTasks = taskMetadata == null ?
            Collections.emptyList() :
            taskMetadata.findTasks(DataFrameTransform.NAME, (t) -> true);
        final int taskCount = dataFrameTasks.size();
        final Map<String, Long> transformsCountByState = new HashMap<>();
        for(PersistentTasksCustomMetaData.PersistentTask<?> dataFrameTask : dataFrameTasks) {
            DataFrameTransformState state = (DataFrameTransformState)dataFrameTask.getState();
            transformsCountByState.merge(state.getTaskState().value(), 1L, Long::sum);
        }

        ActionListener<DataFrameIndexerTransformStats> totalStatsListener = ActionListener.wrap(
            statSummations -> listener.onResponse(new DataFrameFeatureSetUsage(available(),
                enabled(),
                transformsCountByState,
                statSummations)),
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
                    listener.onResponse(new DataFrameFeatureSetUsage(available(),
                        enabled(),
                        transformsCountByState,
                        DataFrameIndexerTransformStats.withDefaultTransformId()));
                    return;
                }
                transformsCountByState.merge(DataFrameTransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
                getStatisticSummations(client, totalStatsListener);
            },
            transformCountFailure -> {
               if (transformCountFailure instanceof ResourceNotFoundException) {
                   getStatisticSummations(client, totalStatsListener);
               } else {
                   listener.onFailure(transformCountFailure);
               }
            }
        );

        SearchRequest totalTransformCount = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
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

    static DataFrameIndexerTransformStats parseSearchAggs(SearchResponse searchResponse) {
        List<Long> statisticsList = new ArrayList<>(PROVIDED_STATS.length);

        for(String statName : PROVIDED_STATS) {
            Aggregation agg = searchResponse.getAggregations().get(statName);

            if (agg instanceof NumericMetricsAggregation.SingleValue) {
                statisticsList.add((long)((NumericMetricsAggregation.SingleValue)agg).value());
            } else {
                statisticsList.add(0L);
            }
        }
        return DataFrameIndexerTransformStats.withDefaultTransformId(statisticsList.get(0),  // numPages
            statisticsList.get(1),  // numInputDocuments
            statisticsList.get(2),  // numOutputDocuments
            statisticsList.get(3),  // numInvocations
            statisticsList.get(4),  // indexTime
            statisticsList.get(5),  // searchTime
            statisticsList.get(6),  // indexTotal
            statisticsList.get(7),  // searchTotal
            statisticsList.get(8),  // indexFailures
            statisticsList.get(9)); // searchFailures
    }

    static void getStatisticSummations(Client client, ActionListener<DataFrameIndexerTransformStats> statsListener) {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(),
                    DataFrameTransformStateAndStats.NAME)));

        SearchRequestBuilder requestBuilder = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .setSize(0)
            .setQuery(queryBuilder);

        final String path = DataFrameField.STATS_FIELD.getPreferredName() + ".";
        for(String statName : PROVIDED_STATS) {
            requestBuilder.addAggregation(AggregationBuilders.sum(statName).field(path + statName));
        }

        ActionListener<SearchResponse> getStatisticSummationsListener = ActionListener.wrap(
            searchResponse -> {
                if (searchResponse.getShardFailures().length > 0) {
                    logger.error("statistics summations search returned shard failures: {}",
                        Arrays.toString(searchResponse.getShardFailures()));
                }

                statsListener.onResponse(parseSearchAggs(searchResponse));
            },
            failure -> {
                if (failure instanceof ResourceNotFoundException) {
                    statsListener.onResponse(DataFrameIndexerTransformStats.withDefaultTransformId());
                } else {
                    statsListener.onFailure(failure);
                }
            }
        );
        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            requestBuilder.request(),
            getStatisticSummationsListener,
            client::search);
    }
}
