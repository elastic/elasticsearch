/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.SingleGroupSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to gather the progress information for a given config and its cursor position
 */
public final class TransformProgressGatherer {

    private static final Logger logger = LogManager.getLogger(TransformProgressGatherer.class);

    /**
     * This gathers the progress of the current indexer given its position.
     *
     * @param progressListener The listener to alert on completion. Error is returned if the config has not been set on the indexer
     */
    public static void getProgress(Client client,
                                   DataFrameTransformConfig config,
                                   Map<String, Object> cursor,
                                   ActionListener<DataFrameTransformProgress> progressListener) {
        MultiSearchRequestBuilder request = client.prepareMultiSearch()
            .add(client.prepareSearch(config.getSource().getIndex())
                .setSize(0)
                .setTrackTotalHits(true)
                .setQuery(config.getSource().getQueryConfig().getQuery())
                .request());

        if (cursor != null) {
            request.add(client.prepareSearch(config.getSource().getIndex())
                .setSize(0)
                .setTrackTotalHits(true)
                .setQuery(docsLeftQuery(config, cursor))
                .request());
        }
        ActionListener<MultiSearchResponse> multiSearchResponseActionListener = ActionListener.wrap(
            mSearchResponse -> {
                long[] counts = new long[]{0, 0};
                for (int i = 0; i < mSearchResponse.getResponses().length; i++) {
                    MultiSearchResponse.Item item = mSearchResponse.getResponses()[i];
                    if (item.isFailure()) {
                        progressListener.onFailure(new ElasticsearchException("Failed gather transform task progress", item.getFailure()));
                        return;
                    }
                    counts[i] = item.getResponse().getHits().getTotalHits().value;
                }
                // It stands to reason that total document count will be greater than the remaining document count as the
                // remaining document count query contains the user provided query as a filter, thus should at most have the same
                // amount of documents as the total document count
                // If the mSearchResponse only contains 1 response, that response is the total document count
                long totalDocs = Math.max(counts[0], counts[1]);
                long remainingDocs = mSearchResponse.getResponses().length == 1 ? totalDocs : Math.min(counts[0], counts[1]);
                progressListener.onResponse(new DataFrameTransformProgress(totalDocs, remainingDocs));
            },
            progressListener::onFailure
        );
        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            request.request(),
            multiSearchResponseActionListener, client::multiSearch);
    }

    private static QueryBuilder docsLeftQuery(DataFrameTransformConfig config, Map<String, Object> cursor) {
        if (cursor == null) {
            return config.getSource().getQueryConfig().getQuery();
        }
        Map<String, SingleGroupSource<?>> groupConfig = config.getPivotConfig().getGroupConfig().getGroups();
        List<Map.Entry<String, Object>> searchedGroupings = new ArrayList<>(cursor.size());
        BoolQueryBuilder shouldQueries = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> grouping : cursor.entrySet()) {
            BoolQueryBuilder filterQueries = QueryBuilders.boolQuery();
            SingleGroupSource<?> groupSource = groupConfig.get(grouping.getKey());
            if (groupSource == null) {
                logger.warn("null group source for key [" + grouping.getKey() + "]");
                continue;
            }
            if (grouping.getValue() == null) {
                logger.warn("null cursor value for key [" + grouping.getKey() + "]");
                continue;
            }
            filterQueries.filter(groupSource.getNextBucketsQuery(grouping.getValue()));
            searchedGroupings.forEach(kv -> filterQueries.filter(groupConfig.get(kv.getKey()).getCurrentBucketQuery(kv.getValue())));
            searchedGroupings.add(grouping);
            shouldQueries.should(filterQueries);
        }

        return QueryBuilders.boolQuery()
            .filter(config.getSource().getQueryConfig().getQuery())
            .filter(shouldQueries);
    }

}
