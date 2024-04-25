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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class CoordinatorRewriteContextProvider {
    private final XContentParserConfiguration parserConfig;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final Function<Index, Map<String, DateFieldMapper.DateFieldType>> mappingSupplier;

    public CoordinatorRewriteContextProvider(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        Supplier<ClusterState> clusterStateSupplier,
        Function<Index, Map<String, DateFieldMapper.DateFieldType>> mappingSupplier
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
        Map<String, DateFieldMapper.DateFieldType> dateFieldMap = mappingSupplier.apply(index);
        if (dateFieldMap == null) {
            return null;
        }

        DateFieldMapper.DateFieldType timestampFieldType = dateFieldMap.get(DataStream.TIMESTAMP_FIELD_NAME);
        IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        /// MP TODO: should we also check eventIngestedRange.containsAllShardRanges() == false ??
        if (timestampRange.containsAllShardRanges() == false) {
            timestampRange = indexMetadata.getTimeSeriesTimestampRange(timestampFieldType);
            if (timestampRange == null) {
                /// MP TODO: does this logic need to change now? - can we short circuit without checking event.ingested (?)
                return null;
            }
        }

        DateFieldMapper.DateFieldType eventIngestedFieldType = dateFieldMap.get(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
        IndexLongFieldRange eventIngestedRange = indexMetadata.getEventIngestedRange();

        var atTimestampRangeInfo = new CoordinatorRewriteContext.DateFieldRange(timestampFieldType, timestampRange);
        var eventIngestedRangeInfo = new CoordinatorRewriteContext.DateFieldRange(eventIngestedFieldType, eventIngestedRange);
        return new CoordinatorRewriteContext(parserConfig, client, nowInMillis, atTimestampRangeInfo, eventIngestedRangeInfo);
    }
}
