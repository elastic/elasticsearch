/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class MappingAwareRewriteContextProvider implements Supplier<MappingAwareRewriteContext> {
    private final XContentParserConfiguration parserConfig;
    private final NamedWriteableRegistry writeableRegistry;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final IndexService indexService;
    private final ShardId shardId;
    private final int shardRequestIndex;
    private final String clusterAlias;

    public MappingAwareRewriteContextProvider(
        final XContentParserConfiguration parserConfig,
        final NamedWriteableRegistry writeableRegistry,
        final Client client,
        final LongSupplier nowInMillis,
        final IndexService indexService,
        final ShardId shardId,
        int shardRequestIndex,
        final String clusterAlias
    ) {
        this.parserConfig = parserConfig;
        this.writeableRegistry = writeableRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.indexService = indexService;
        this.shardId = shardId;
        this.shardRequestIndex = shardRequestIndex;
        this.clusterAlias = clusterAlias;
    }

    @Override
    public MappingAwareRewriteContext get() {
        return new MappingAwareRewriteContext(
            parserConfig,
            writeableRegistry,
            client,
            nowInMillis,
            indexService,
            shardId,
            shardRequestIndex,
            clusterAlias
        );
    }
}
