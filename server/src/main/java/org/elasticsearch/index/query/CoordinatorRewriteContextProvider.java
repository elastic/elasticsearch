/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.CachedTimestampFieldInfo;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class CoordinatorRewriteContextProvider {
    private final XContentParserConfiguration parserConfig;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final Function<Index, CachedTimestampFieldInfo> mappingSupplier;

    public CoordinatorRewriteContextProvider(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        Supplier<ClusterState> clusterStateSupplier,
        Function<Index, CachedTimestampFieldInfo> mappingSupplier
    ) {
        this.parserConfig = parserConfig;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.clusterStateSupplier = clusterStateSupplier;
        this.mappingSupplier = mappingSupplier;
    }

    @Nullable
    public CoordinatorRewriteContext getCoordinatorRewriteContext(Index index) {
        var clusterState = clusterStateSupplier.get();
        var indexMetadata = clusterState.metadata().index(index);

        if (indexMetadata == null) {
            return null;
        }
        CachedTimestampFieldInfo timestampsFieldInfo = mappingSupplier.apply(index);
        if (timestampsFieldInfo == null) {
            return null;
        }

        // ensure the cached info has the latest ranges from cluster state
        timestampsFieldInfo.setTimestampRange(indexMetadata.getTimestampRange());
        timestampsFieldInfo.setEventIngestedRange(indexMetadata.getEventIngestedRange());

        if (timestampsFieldInfo.getTimestampRange().containsAllShardRanges() == false) {
            // if @timestamp range is not present or not ready in cluster state, fallback to using time series range (if present)
            timestampsFieldInfo.setTimestampRange(indexMetadata.getTimeSeriesTimestampRange(timestampsFieldInfo.getTimestampFieldType()));
            // if timestampRange in the time series is null AND the eventIngestedRange is not ready for use, return null (no coord rewrite)
            if (timestampsFieldInfo.getTimestampRange() == null
                && timestampsFieldInfo.getEventIngestedRange().containsAllShardRanges() == false) {
                return null;
            }
        }

        return new CoordinatorRewriteContext(parserConfig, client, nowInMillis, timestampsFieldInfo);
    }
}
