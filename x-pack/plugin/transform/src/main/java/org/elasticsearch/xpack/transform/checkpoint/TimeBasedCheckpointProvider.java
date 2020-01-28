/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

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
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

public class TimeBasedCheckpointProvider extends DefaultCheckpointProvider {

    private static final Logger logger = LogManager.getLogger(TimeBasedCheckpointProvider.class);

    private final TimeSyncConfig timeSyncConfig;

    TimeBasedCheckpointProvider(
        final Client client,
        final RemoteClusterResolver remoteClusterResolver,
        final TransformConfigManager transformConfigManager,
        final TransformAuditor transformAuditor,
        final TransformConfig transformConfig
    ) {
        super(client, remoteClusterResolver, transformConfigManager, transformAuditor, transformConfig);
        timeSyncConfig = (TimeSyncConfig) transformConfig.getSyncConfig();
    }

    @Override
    public void sourceHasChanged(TransformCheckpoint lastCheckpoint, ActionListener<Boolean> listener) {

        final long timestamp = getTime();

        SearchRequest searchRequest = new SearchRequest(transformConfig.getSource().getIndex()).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0)
            // we only want to know if there is at least 1 new document
            .trackTotalHitsUpTo(1);

        QueryBuilder queryBuilder = transformConfig.getSource().getQueryConfig().getQuery();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder)
            .filter(
                new RangeQueryBuilder(timeSyncConfig.getField()).gte(lastCheckpoint.getTimeUpperBound())
                    .lt(timestamp - timeSyncConfig.getDelay().millis())
                    .format("epoch_millis")
            );

        sourceBuilder.query(filteredQuery);
        searchRequest.source(sourceBuilder);

        logger.trace("query for changes based on time: {}", sourceBuilder);

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequest,
            ActionListener.wrap(r -> { listener.onResponse(r.getHits().getTotalHits().value > 0L); }, listener::onFailure)
        );
    }

    @Override
    public void createNextCheckpoint(final TransformCheckpoint lastCheckpoint, final ActionListener<TransformCheckpoint> listener) {
        final long timestamp = getTime();
        final long checkpoint = TransformCheckpoint.isNullOrEmpty(lastCheckpoint) ? 1 : lastCheckpoint.getCheckpoint() + 1;

        // for time based synchronization
        long timeUpperBound = timestamp - timeSyncConfig.getDelay().millis();

        getIndexCheckpoints(
            ActionListener.wrap(
                checkpointsByIndex -> {
                    listener.onResponse(
                        new TransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, timeUpperBound)
                    );
                },
                listener::onFailure
            )
        );
    }

    // for the purpose of testing
    long getTime() {
        return System.currentTimeMillis();
    }
}
