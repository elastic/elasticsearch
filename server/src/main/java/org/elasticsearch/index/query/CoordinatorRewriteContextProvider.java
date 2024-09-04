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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class CoordinatorRewriteContextProvider {
    private final XContentParserConfiguration parserConfig;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final Function<Index, DateFieldRangeInfo> mappingSupplier;

    public CoordinatorRewriteContextProvider(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        Supplier<ClusterState> clusterStateSupplier,
        Function<Index, DateFieldRangeInfo> mappingSupplier
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
        DateFieldRangeInfo dateFieldRangeInfo = mappingSupplier.apply(index);
        if (dateFieldRangeInfo == null) {
            return null;
        }
        DateFieldMapper.DateFieldType timestampFieldType = dateFieldRangeInfo.timestampFieldType();
        IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        IndexLongFieldRange eventIngestedRange = indexMetadata.getEventIngestedRange();

        if (timestampRange.containsAllShardRanges() == false) {
            // if @timestamp range is not present or not ready in cluster state, fallback to using time series range (if present)
            timestampRange = indexMetadata.getTimeSeriesTimestampRange(timestampFieldType);
            // if timestampRange in the time series is null AND the eventIngestedRange is not ready for use, return null (no coord rewrite)
            if (timestampRange == null && eventIngestedRange.containsAllShardRanges() == false) {
                return null;
            }
        }

        // the DateFieldRangeInfo from the mappingSupplier only has field types, but not ranges
        // so create a new object with ranges pulled from cluster state
        return new CoordinatorRewriteContext(
            parserConfig,
            client,
            nowInMillis,
            new DateFieldRangeInfo(timestampFieldType, timestampRange, dateFieldRangeInfo.eventIngestedFieldType(), eventIngestedRange)
        );
    }
}
