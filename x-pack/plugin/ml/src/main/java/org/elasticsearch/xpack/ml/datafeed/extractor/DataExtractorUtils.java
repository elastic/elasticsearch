/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Utility methods for various DataExtractor implementations.
 */
public final class DataExtractorUtils {

    private static final String EPOCH_MILLIS = "epoch_millis";
    private static final String EARLIEST_TIME = "earliest_time";
    private static final String LATEST_TIME = "latest_time";

    private DataExtractorUtils() {}

    /**
     * Combines a user query with a time range query.
     */
    public static QueryBuilder wrapInTimeRangeQuery(QueryBuilder query, String timeField, long start, long end) {
        QueryBuilder timeQuery = new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
        return new BoolQueryBuilder().filter(query).filter(timeQuery);
    }

    public static SearchRequestBuilder getSearchRequestBuilderForSummary(Client client, DataExtractorQueryContext context) {
        return new SearchRequestBuilder(client).setIndices(context.indices)
            .setIndicesOptions(context.indicesOptions)
            .setSource(getSearchSourceBuilderForSummary(context))
            .setAllowPartialSearchResults(false)
            .setTrackTotalHits(true);
    }

    public static SearchSourceBuilder getSearchSourceBuilderForSummary(DataExtractorQueryContext context) {
        return new SearchSourceBuilder().size(0)
            .query(DataExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, context.start, context.end))
            .runtimeMappings(context.runtimeMappings)
            .aggregation(AggregationBuilders.min(EARLIEST_TIME).field(context.timeField))
            .aggregation(AggregationBuilders.max(LATEST_TIME).field(context.timeField));
    }

    public static DataExtractor.DataSummary getDataSummary(SearchResponse searchResponse) {
        InternalAggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return new DataExtractor.DataSummary(null, null, 0L);
        } else {
            Long earliestTime = toLongIfFinite((aggregations.<Min>get(EARLIEST_TIME)).value());
            Long latestTime = toLongIfFinite((aggregations.<Max>get(LATEST_TIME)).value());
            long totalHits = searchResponse.getHits().getTotalHits().value();
            return new DataExtractor.DataSummary(earliestTime, latestTime, totalHits);
        }
    }

    /**
     * The min and max aggregations return infinity when there is no data. To ensure consistency
     * between the different types of data summary we represent no data by earliest and latest times
     * being <code>null</code>. Hence, this method converts infinite values to <code>null</code>.
     */
    private static Long toLongIfFinite(double x) {
        return Double.isFinite(x) ? (long) x : null;
    }

    /**
     * Check whether the search skipped CCS clusters.
     * @throws ResourceNotFoundException if any CCS clusters were skipped, as this could
     *                                   cause anomalies to be spuriously detected.
     * @param searchResponse The search response to check for skipped CCS clusters.
     */
    public static void checkForSkippedClusters(SearchResponse searchResponse) {
        SearchResponse.Clusters clusterResponse = searchResponse.getClusters();
        if (clusterResponse != null && clusterResponse.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED) > 0) {
            throw new ResourceNotFoundException(
                "[{}] remote clusters out of [{}] were skipped when performing datafeed search",
                clusterResponse.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED),
                clusterResponse.getTotal()
            );
        }
    }
}
