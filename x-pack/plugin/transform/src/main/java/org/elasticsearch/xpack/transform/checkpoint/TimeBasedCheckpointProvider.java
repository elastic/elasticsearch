/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static java.util.function.Function.identity;

class TimeBasedCheckpointProvider extends DefaultCheckpointProvider {

    private static final Logger logger = LogManager.getLogger(TimeBasedCheckpointProvider.class);

    private final TimeSyncConfig timeSyncConfig;
    // function aligning the given timestamp with date histogram interval or identity function is aligning is not possible
    private final Function<Long, Long> alignTimestamp;

    TimeBasedCheckpointProvider(
        final Clock clock,
        final ParentTaskAssigningClient client,
        final RemoteClusterResolver remoteClusterResolver,
        final TransformConfigManager transformConfigManager,
        final TransformAuditor transformAuditor,
        final TransformConfig transformConfig
    ) {
        super(clock, client, remoteClusterResolver, transformConfigManager, transformAuditor, transformConfig);
        timeSyncConfig = (TimeSyncConfig) transformConfig.getSyncConfig();
        alignTimestamp = createAlignTimestampFunction(transformConfig);
    }

    @Override
    public void sourceHasChanged(TransformCheckpoint lastCheckpoint, ActionListener<Boolean> listener) {
        final long timestamp = clock.millis();
        final long timeUpperBound = alignTimestamp.apply(timestamp - timeSyncConfig.getDelay().millis());

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(transformConfig.getSource().getQueryConfig().getQuery())
            .filter(
                new RangeQueryBuilder(timeSyncConfig.getField()).gte(lastCheckpoint.getTimeUpperBound())
                    .lt(timeUpperBound)
                    .format("epoch_millis")
            );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0)
            // we only want to know if there is at least 1 new document
            .trackTotalHitsUpTo(1)
            .query(queryBuilder);
        SearchRequest searchRequest = new SearchRequest(transformConfig.getSource().getIndex()).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .source(sourceBuilder);

        logger.trace("query for changes based on time: {}", sourceBuilder);

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            searchRequest,
            ActionListener.wrap(r -> listener.onResponse(r.getHits().getTotalHits().value > 0L), listener::onFailure)
        );
    }

    @Override
    public void createNextCheckpoint(final TransformCheckpoint lastCheckpoint, final ActionListener<TransformCheckpoint> listener) {
        final long timestamp = clock.millis();
        final long checkpoint = TransformCheckpoint.isNullOrEmpty(lastCheckpoint) ? 1 : lastCheckpoint.getCheckpoint() + 1;

        // for time based synchronization
        final long timeUpperBound = alignTimestamp.apply(timestamp - timeSyncConfig.getDelay().millis());

        getIndexCheckpoints(INTERNAL_GET_INDEX_CHECKPOINTS_TIMEOUT, ActionListener.wrap(checkpointsByIndex -> {
            listener.onResponse(
                new TransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, timeUpperBound)
            );
        }, listener::onFailure));
    }

    /**
     * Aligns the timestamp with date histogram group source interval (if it is provided).
     *
     * @param transformConfig transform configuration
     * @return function aligning the given timestamp with date histogram interval
     */
    private static Function<Long, Long> createAlignTimestampFunction(TransformConfig transformConfig) {
        if (TransformEffectiveSettings.isAlignCheckpointsDisabled(transformConfig.getSettings())) {
            return identity();
        }
        // In case of transforms created before aligning timestamp optimization was introduced we assume the default was "false".
        if (transformConfig.getVersion() == null || transformConfig.getVersion().before(TransformConfigVersion.V_7_15_0)) {
            return identity();
        }
        if (transformConfig.getPivotConfig() == null) {
            return identity();
        }
        if (transformConfig.getPivotConfig().getGroupConfig() == null) {
            return identity();
        }
        Map<String, SingleGroupSource> groups = transformConfig.getPivotConfig().getGroupConfig().getGroups();
        if (groups == null || groups.isEmpty()) {
            return identity();
        }
        Optional<DateHistogramGroupSource> dateHistogramGroupSource = groups.values()
            .stream()
            .filter(DateHistogramGroupSource.class::isInstance)
            .map(DateHistogramGroupSource.class::cast)
            .filter(group -> Objects.equals(group.getField(), transformConfig.getSyncConfig().getField()))
            .findFirst();
        if (dateHistogramGroupSource.isEmpty()) {
            return identity();
        }
        return dateHistogramGroupSource.get().getRounding()::round;
    }
}
