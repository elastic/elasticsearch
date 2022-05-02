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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class CoordinatorRewriteContextProvider {
    private final XContentParserConfiguration parserConfig;
    private final NamedWriteableRegistry writeableRegistry;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final Function<Index, DateFieldMapper.DateFieldType> mappingSupplier;

    public CoordinatorRewriteContextProvider(
        XContentParserConfiguration parserConfig,
        NamedWriteableRegistry writeableRegistry,
        Client client,
        LongSupplier nowInMillis,
        Supplier<ClusterState> clusterStateSupplier,
        Function<Index, DateFieldMapper.DateFieldType> mappingSupplier
    ) {
        this.parserConfig = parserConfig;
        this.writeableRegistry = writeableRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.clusterStateSupplier = clusterStateSupplier;
        this.mappingSupplier = mappingSupplier;
    }

    @Nullable
    public CoordinatorRewriteContext getCoordinatorRewriteContext(Index index) {
        ClusterState clusterState = clusterStateSupplier.get();
        IndexMetadata indexMetadata = clusterState.metadata().index(index);

        if (indexMetadata == null || indexMetadata.getTimestampRange().containsAllShardRanges() == false) {
            return null;
        }

        DateFieldMapper.DateFieldType dateFieldType = mappingSupplier.apply(index);

        if (dateFieldType == null) {
            return null;
        }

        IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        return new CoordinatorRewriteContext(parserConfig, writeableRegistry, client, nowInMillis, index, timestampRange, dateFieldType);
    }
}
