/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

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
import org.elasticsearch.xpack.core.transform.TransformFeatureSetUsage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TransformFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    private static final Logger logger = LogManager.getLogger(TransformFeatureSet.class);

    public static final String[] PROVIDED_STATS = new String[] {
        TransformIndexerStats.NUM_PAGES.getPreferredName(),
        TransformIndexerStats.NUM_INPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_INVOCATIONS.getPreferredName(),
        TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.INDEX_TOTAL.getPreferredName(),
        TransformIndexerStats.SEARCH_TOTAL.getPreferredName(),
        TransformIndexerStats.INDEX_FAILURES.getPreferredName(),
        TransformIndexerStats.SEARCH_FAILURES.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(), };

    @Inject
    public TransformFeatureSet(Settings settings, ClusterService clusterService, Client client, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.TRANSFORM_ENABLED.get(settings);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.TRANSFORM;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isTransformAllowed();
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
            listener.onResponse(new TransformFeatureSetUsage(available(), enabled(), Collections.emptyMap(), new TransformIndexerStats()));
            return;
        }

        PersistentTasksCustomMetaData taskMetadata = PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterService.state());
        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> transformTasks = taskMetadata == null
            ? Collections.emptyList()
            : taskMetadata.findTasks(TransformTaskParams.NAME, (t) -> true);
        final int taskCount = transformTasks.size();
        final Map<String, Long> transformsCountByState = new HashMap<>();
        for (PersistentTasksCustomMetaData.PersistentTask<?> transformTask : transformTasks) {
            TransformState state = (TransformState) transformTask.getState();
            transformsCountByState.merge(state.getTaskState().value(), 1L, Long::sum);
        }

        ActionListener<TransformIndexerStats> totalStatsListener = ActionListener.wrap(
            statSummations -> listener.onResponse(
                new TransformFeatureSetUsage(available(), enabled(), transformsCountByState, statSummations)
            ),
            listener::onFailure
        );

        ActionListener<SearchResponse> totalTransformCountListener = ActionListener.wrap(transformCountSuccess -> {
            if (transformCountSuccess.getShardFailures().length > 0) {
                logger.error(
                    "total transform count search returned shard failures: {}",
                    Arrays.toString(transformCountSuccess.getShardFailures())
                );
            }
            long totalTransforms = transformCountSuccess.getHits().getTotalHits().value;
            if (totalTransforms == 0) {
                listener.onResponse(
                    new TransformFeatureSetUsage(available(), enabled(), transformsCountByState, new TransformIndexerStats())
                );
                return;
            }
            transformsCountByState.merge(TransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
            getStatisticSummations(client, totalStatsListener);
        }, transformCountFailure -> {
            if (transformCountFailure instanceof ResourceNotFoundException) {
                getStatisticSummations(client, totalStatsListener);
            } else {
                listener.onFailure(transformCountFailure);
            }
        });

        SearchRequest totalTransformCount = client.prepareSearch(TransformInternalIndexConstants.INDEX_NAME_PATTERN)
            .setTrackTotalHits(true)
            .setQuery(
                QueryBuilders.constantScoreQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformConfig.NAME))
                )
            )
            .request();

        ClientHelper.executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.TRANSFORM_ORIGIN,
            totalTransformCount,
            totalTransformCountListener,
            client::search
        );
    }

    static TransformIndexerStats parseSearchAggs(SearchResponse searchResponse) {
        List<Double> statisticsList = new ArrayList<>(PROVIDED_STATS.length);

        for (String statName : PROVIDED_STATS) {
            Aggregation agg = searchResponse.getAggregations().get(statName);

            if (agg instanceof NumericMetricsAggregation.SingleValue) {
                statisticsList.add(((NumericMetricsAggregation.SingleValue) agg).value());
            } else {
                statisticsList.add(0.0);
            }
        }
        return new TransformIndexerStats(
            statisticsList.get(0).longValue(),  // numPages
            statisticsList.get(1).longValue(),  // numInputDocuments
            statisticsList.get(2).longValue(),  // numOutputDocuments
            statisticsList.get(3).longValue(),  // numInvocations
            statisticsList.get(4).longValue(),  // indexTime
            statisticsList.get(5).longValue(),  // searchTime
            statisticsList.get(6).longValue(),  // indexTotal
            statisticsList.get(7).longValue(),  // searchTotal
            statisticsList.get(8).longValue(),  // indexFailures
            statisticsList.get(9).longValue(),  // searchFailures
            statisticsList.get(10), // exponential_avg_checkpoint_duration_ms
            statisticsList.get(11), // exponential_avg_documents_indexed
            statisticsList.get(12)  // exponential_avg_documents_processed
        );
    }

    static void getStatisticSummations(Client client, ActionListener<TransformIndexerStats> statsListener) {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformStoredDoc.NAME))
        );

        SearchRequestBuilder requestBuilder = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        ).setSize(0).setQuery(queryBuilder);

        final String path = TransformField.STATS_FIELD.getPreferredName() + ".";
        for (String statName : PROVIDED_STATS) {
            requestBuilder.addAggregation(AggregationBuilders.sum(statName).field(path + statName));
        }

        ActionListener<SearchResponse> getStatisticSummationsListener = ActionListener.wrap(searchResponse -> {
            if (searchResponse.getShardFailures().length > 0) {
                logger.error(
                    "statistics summations search returned shard failures: {}",
                    Arrays.toString(searchResponse.getShardFailures())
                );
            }

            statsListener.onResponse(parseSearchAggs(searchResponse));
        }, failure -> {
            if (failure instanceof ResourceNotFoundException) {
                statsListener.onResponse(new TransformIndexerStats());
            } else {
                statsListener.onFailure(failure);
            }
        });
        ClientHelper.executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.TRANSFORM_ORIGIN,
            requestBuilder.request(),
            getStatisticSummationsListener,
            client::search
        );
    }
}
