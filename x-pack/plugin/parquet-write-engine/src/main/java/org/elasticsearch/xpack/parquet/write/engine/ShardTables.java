/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.IOException;

/**
 * Gives an engine the Iceberg table of its index. The engine asks for it at its first flush and not when it starts, so that a shard
 * starts even when the repository is not reachable yet, and the rows of its bulks stay safe on the local disk until it is.
 */
@FunctionalInterface
public interface ShardTables {

    /**
     * @param historyUuid the history of the shard in its store: a shard with a new store has a new one, and its files are kept apart
     * @throws IOException if the repository cannot be used, for example because it does not exist or is not started
     */
    ShardTable open(ShardId shardId, Settings indexSettings, String historyUuid) throws IOException;
}
