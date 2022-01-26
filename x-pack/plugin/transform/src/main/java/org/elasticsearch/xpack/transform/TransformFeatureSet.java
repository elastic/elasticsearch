/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
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
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class TransformFeatureSet implements XPackFeatureSet {

    private final Client client;
    private final ClusterService clusterService;

    private static final Logger logger = LogManager.getLogger(TransformFeatureSet.class);

    private static final String FEATURE_COUNTS = "feature_counts";

    /**
     * Features we want to measure the usage of.
     *
     * Each feature corresponds to a field in {@link TransformConfig}.
     * If the field exists in the config then we assume the feature is used.
     */
    private static final String[] FEATURES = Stream.concat(
        Stream.of(TransformConfig.Function.values()).map(TransformConfig.Function::getParseField),
        Stream.of(TransformField.RETENTION_POLICY, TransformField.SYNC)
    ).map(ParseField::getPreferredName).toArray(String[]::new);

    public static final String[] PROVIDED_STATS = new String[] {
        TransformIndexerStats.NUM_PAGES.getPreferredName(),
        TransformIndexerStats.NUM_INPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_DELETED_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_INVOCATIONS.getPreferredName(),
        TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.DELETE_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.INDEX_TOTAL.getPreferredName(),
        TransformIndexerStats.SEARCH_TOTAL.getPreferredName(),
        TransformIndexerStats.PROCESSING_TOTAL.getPreferredName(),
        TransformIndexerStats.INDEX_FAILURES.getPreferredName(),
        TransformIndexerStats.SEARCH_FAILURES.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(), };

    @Inject
    public TransformFeatureSet(ClusterService clusterService, Client client) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public String name() {
        return XPackField.TRANSFORM;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        PersistentTasksCustomMetadata taskMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterService.state());
        Collection<PersistentTasksCustomMetadata.PersistentTask<?>> transformTasks = taskMetadata == null
            ? Collections.emptyList()
            : taskMetadata.findTasks(TransformTaskParams.NAME, t -> true);
        final int taskCount = transformTasks.size();
        final Map<String, Long> transformsCountByState = new HashMap<>();
        for (PersistentTasksCustomMetadata.PersistentTask<?> transformTask : transformTasks) {
            TransformState state = (TransformState) transformTask.getState();
            transformsCountByState.merge(state.getTaskState().value(), 1L, Long::sum);
        }
        final SetOnce<Map<String, Long>> transformsCountByFeature = new SetOnce<>();

        ActionListener<TransformIndexerStats> totalStatsListener = ActionListener.wrap(
            statSummations -> listener.onResponse(
                new TransformFeatureSetUsage(transformsCountByState, transformsCountByFeature.get(), statSummations)
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
                    new TransformFeatureSetUsage(transformsCountByState, Collections.emptyMap(), new TransformIndexerStats())
                );
                return;
            }
            transformsCountByState.merge(TransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
            transformsCountByFeature.set(getFeatureCounts(transformCountSuccess.getAggregations()));
            getStatisticSummations(client, totalStatsListener);
        }, transformCountFailure -> {
            if (transformCountFailure instanceof ResourceNotFoundException) {
                getStatisticSummations(client, totalStatsListener);
            } else {
                listener.onFailure(transformCountFailure);
            }
        });

        SearchRequest totalTransformCountSearchRequest = client.prepareSearch(TransformInternalIndexConstants.INDEX_NAME_PATTERN)
            .setTrackTotalHits(true)
            // We only need the total hits count and aggs.
            .setSize(0)
            .setFetchSource(false)
            .setQuery(
                QueryBuilders.constantScoreQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformConfig.NAME))
                )
            )
            .addAggregation(
                AggregationBuilders.filters(
                    FEATURE_COUNTS,
                    Arrays.stream(FEATURES)
                        .map(f -> new FiltersAggregator.KeyedFilter(f, QueryBuilders.existsQuery(f)))
                        .toArray(FiltersAggregator.KeyedFilter[]::new)
                )
            )
            .request();

        ClientHelper.executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.TRANSFORM_ORIGIN,
            totalTransformCountSearchRequest,
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
            statisticsList.get(3).longValue(),  // numDeletedDocuments
            statisticsList.get(4).longValue(),  // numInvocations
            statisticsList.get(5).longValue(),  // indexTime
            statisticsList.get(6).longValue(),  // searchTime
            statisticsList.get(7).longValue(),  // processingTime
            statisticsList.get(8).longValue(),  // deleteTime
            statisticsList.get(9).longValue(),  // indexTotal
            statisticsList.get(10).longValue(),  // searchTotal
            statisticsList.get(11).longValue(),  // processingTotal
            statisticsList.get(12).longValue(),  // indexFailures
            statisticsList.get(13).longValue(), // searchFailures
            statisticsList.get(14), // exponential_avg_checkpoint_duration_ms
            statisticsList.get(15), // exponential_avg_documents_indexed
            statisticsList.get(16)  // exponential_avg_documents_processed
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

    /**
     * Returns the feature usage map.
     * For each feature it counts the number of transforms using this feature.
     *
     * @param aggs aggs returned by the search
     * @return feature usage map
     */
    private static Map<String, Long> getFeatureCounts(Aggregations aggs) {
        Filters filters = aggs.get(FEATURE_COUNTS);
        return filters.getBuckets().stream().collect(toMap(Filters.Bucket::getKeyAsString, Filters.Bucket::getDocCount));
    }
}
