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
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.transform.TransformFeatureSetUsage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class TransformUsageTransportAction extends XPackUsageFeatureTransportAction {

    private static final Logger logger = LogManager.getLogger(TransformUsageTransportAction.class);

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

    private final Client client;

    @Inject
    public TransformUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.TRANSFORM.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState clusterState,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        Collection<PersistentTasksCustomMetadata.PersistentTask<?>> transformTasks = TransformTask.findAllTransformTasks(clusterState);
        final int taskCount = transformTasks.size();
        final Map<String, Long> transformsCountByState = new HashMap<>();
        for (PersistentTasksCustomMetadata.PersistentTask<?> transformTask : transformTasks) {
            TransformState transformState = (TransformState) transformTask.getState();
            Optional.ofNullable(transformState)
                .map(TransformState::getTaskState)
                .map(TransformTaskState::value)
                .ifPresent(value -> transformsCountByState.merge(value, 1L, Long::sum));
        }
        final SetOnce<Map<String, Long>> transformsCountByFeature = new SetOnce<>();

        ActionListener<TransformIndexerStats> totalStatsListener = listener.delegateFailureAndWrap((l, statSummations) -> {
            var usage = new TransformFeatureSetUsage(transformsCountByState, transformsCountByFeature.get(), statSummations);
            l.onResponse(new XPackUsageFeatureResponse(usage));
        });

        ActionListener<SearchResponse> totalTransformCountListener = ActionListener.wrap(transformCountSuccess -> {
            if (transformCountSuccess.getShardFailures().length > 0) {
                logger.error(
                    "total transform count search returned shard failures: {}",
                    Arrays.toString(transformCountSuccess.getShardFailures())
                );
            }
            long totalTransforms = transformCountSuccess.getHits().getTotalHits().value();
            if (totalTransforms == 0) {
                var usage = new TransformFeatureSetUsage(transformsCountByState, Collections.emptyMap(), new TransformIndexerStats());
                listener.onResponse(new XPackUsageFeatureResponse(usage));
                return;
            }
            transformsCountByState.merge(TransformTaskState.STOPPED.value(), totalTransforms - taskCount, Long::sum);
            transformsCountByFeature.set(getFeatureCounts(transformCountSuccess.getAggregations()));
            TransformInfoTransportAction.getStatisticSummations(client, totalStatsListener);
        }, transformCountFailure -> {
            if (transformCountFailure instanceof ResourceNotFoundException) {
                TransformInfoTransportAction.getStatisticSummations(client, totalStatsListener);
            } else {
                listener.onFailure(transformCountFailure);
            }
        });

        SearchRequest totalTransformCountSearchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
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

    /**
     * Returns the feature usage map.
     * For each feature it counts the number of transforms using this feature.
     *
     * @param aggs aggs returned by the search
     * @return feature usage map
     */
    private static Map<String, Long> getFeatureCounts(InternalAggregations aggs) {
        Filters filters = aggs.get(FEATURE_COUNTS);
        return filters.getBuckets().stream().collect(toMap(Filters.Bucket::getKeyAsString, Filters.Bucket::getDocCount));
    }
}
