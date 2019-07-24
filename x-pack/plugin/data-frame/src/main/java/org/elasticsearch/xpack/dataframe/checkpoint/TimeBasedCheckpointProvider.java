/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

public class TimeBasedCheckpointProvider extends DefaultCheckpointProvider {

    private static final Logger logger = LogManager.getLogger(TimeBasedCheckpointProvider.class);

    private final TimeSyncConfig timeSyncConfig;

    TimeBasedCheckpointProvider(Client client,
                                DataFrameTransformsConfigManager dataFrameTransformsConfigManager,
                                DataFrameTransformConfig transformConfig) {
        super(client, dataFrameTransformsConfigManager, transformConfig);
        this.timeSyncConfig = (TimeSyncConfig) transformConfig.getSyncConfig();
    }

    @Override
    public void sourceHasChanged(DataFrameTransformCheckpoint lastCheckpoint,
            ActionListener<Boolean> listener) {

        long timestamp = System.currentTimeMillis();

        SearchRequest searchRequest = new SearchRequest(transformConfig.getSource().getIndex())
                .allowPartialSearchResults(false)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size(0);

        QueryBuilder queryBuilder = transformConfig.getSource().getQueryConfig().getQuery();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().
                filter(queryBuilder).
                filter(new RangeQueryBuilder(timeSyncConfig.getField()).
                        gte(lastCheckpoint.getTimeUpperBound()).
                        lt(timestamp - timeSyncConfig.getDelay().millis()).format("epoch_millis"));

        sourceBuilder.query(filteredQuery);
        searchRequest.source(sourceBuilder);

        logger.trace("query for changes based on time: {}", sourceBuilder);

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, SearchAction.INSTANCE,
                searchRequest, ActionListener.wrap(r -> {
                    listener.onResponse(r.getHits().getTotalHits().value > 0L);
                }, listener::onFailure));
    }

    @Override
    public void getCheckpoint(DataFrameTransformCheckpoint lastCheckpoint, ActionListener<DataFrameTransformCheckpoint> listener) {
        long timestamp = System.currentTimeMillis();
        long checkpoint = lastCheckpoint != null ? lastCheckpoint.getCheckpoint() + 1 : 1;

        // for time based synchronization
        long timeUpperBound = timestamp - timeSyncConfig.getDelay().millis();

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            listener.onResponse(
                    new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, timeUpperBound));
        }, listener::onFailure));
    }
}
