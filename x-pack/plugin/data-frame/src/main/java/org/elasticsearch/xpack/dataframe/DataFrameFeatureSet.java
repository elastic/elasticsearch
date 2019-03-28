/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameFeatureSetUsage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class DataFrameFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    private static final String[] STATS_TO_PROVIDE = new String[] {
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
    public String description() {
        return "Data Frame for the Elastic Stack";
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
                DataFrameIndexerTransformStats.withNullTransformId()));
            return;
        }

        PersistentTasksCustomMetaData taskMetadata = PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterService.state());
        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> dataFrameTasks =
            taskMetadata.findTasks(DataFrameTransform.NAME, (t) -> true);
        int taskCount = dataFrameTasks.size();
        Map<String, Long> transformsCountByState = new HashMap<>();
        DataFrameIndexerTransformStats accumulatedStats = DataFrameIndexerTransformStats.withNullTransformId();
        for(PersistentTasksCustomMetaData.PersistentTask<?> dataFrameTask : dataFrameTasks) {
            DataFrameTransformState state = (DataFrameTransformState)dataFrameTask.getState();
            transformsCountByState.merge(state.getTaskState().value(), 1L, Long::sum);
        }
        ActionListener<SearchResponse> totalTransformCountListener = ActionListener.wrap(
            transformCountSuccess -> {
                long totalTransforms = transformCountSuccess.getHits().getTotalHits().value;
                if (totalTransforms == 0) {
                    listener.onResponse(new DataFrameFeatureSetUsage(available(),
                        enabled(),
                        Collections.emptyMap(),
                        DataFrameIndexerTransformStats.withNullTransformId()));
                    return;
                }
                transformsCountByState.merge(DataFrameTransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
            },
            transformCountFailure -> {
               if (transformCountFailure instanceof ResourceNotFoundException) {
                   listener.onResponse(new DataFrameFeatureSetUsage(available(),
                       enabled(),
                       Collections.emptyMap(),
                       DataFrameIndexerTransformStats.withNullTransformId()));
                   return;
               }
               listener.onFailure(transformCountFailure);
            }
        );

        final GetDataFrameTransformsStatsAction.Request transformStatsRequest = new GetDataFrameTransformsStatsAction.Request(MetaData.ALL);
        SearchRequest totalTransformCount = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformConfig.NAME))))
            .request();

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            totalTransformCount,
            totalTransformCountListener,
            client::search);

        client.execute(GetDataFrameTransformsStatsAction.INSTANCE,
            transformStatsRequest,
            ActionListener.wrap(transformStatsResponse ->
                listener.onResponse(createUsage(available(), enabled(), transformStatsResponse.getTransformsStateAndStats())),
                listener::onFailure));
    }

    static DataFrameFeatureSetUsage createUsage(boolean available,
                                                boolean enabled,
                                                List<DataFrameTransformStateAndStats> transformsStateAndStats) {

        Map<String, Long> transformsCountByState = new HashMap<>();
        DataFrameIndexerTransformStats accumulatedStats = DataFrameIndexerTransformStats.withNullTransformId();
        transformsStateAndStats.forEach(singleResult -> {
            transformsCountByState.merge(singleResult.getTransformState().getIndexerState().value(), 1L, Long::sum);
            accumulatedStats.merge(singleResult.getTransformStats());
        });

        return new DataFrameFeatureSetUsage(available, enabled, transformsCountByState, accumulatedStats);
    }

    static void getStatisticSummations(Client client, ActionListener<DataFrameIndexerTransformStats> statsListener) {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(),
                DataFrameIndexerTransformStats.NAME)));

        SearchRequestBuilder requestBuilder = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .setTrackTotalHits(false)
            .setSize(0)
            .setQuery(queryBuilder);

        for(String statName : STATS_TO_PROVIDE) {
            requestBuilder.addAggregation(AggregationBuilders.sum(statName).field(statName));
        }

        ActionListener<SearchResponse> getStatisticSummationsListener = ActionListener.wrap(
            searchResponse -> {
                List<Long> statisticsList = new ArrayList<>(STATS_TO_PROVIDE.length);

                for(String statName : STATS_TO_PROVIDE) {
                    Aggregation agg = searchResponse.getAggregations().get(statName);
                    if (agg instanceof NumericMetricsAggregation.SingleValue) {
                        statisticsList.add((long)((NumericMetricsAggregation.SingleValue)agg).value());
                    } else {
                        statisticsList.add(0L);
                    }
                }
                DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats(null,
                    statisticsList.get(0),
                    statisticsList.get(1),
                    statisticsList.get(2),
                    statisticsList.get(3),
                    statisticsList.get(4),
                    statisticsList.get(5),
                    statisticsList.get(6),
                    statisticsList.get(7),
                    statisticsList.get(8),
                    statisticsList.get(9));
            },
            failure -> {
                if (failure instanceof ResourceNotFoundException) {
                    statsListener.onResponse(DataFrameIndexerTransformStats.withNullTransformId());
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
