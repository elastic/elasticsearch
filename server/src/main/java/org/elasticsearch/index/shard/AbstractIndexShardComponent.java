/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

public abstract class AbstractIndexShardComponent implements IndexShardComponent {

    protected final Logger logger;
    protected volatile ShardId shardId;
    protected final IndexSettings indexSettings;

    protected AbstractIndexShardComponent(ShardId shardId, IndexSettings indexSettings) {
        this.shardId = shardId;
        this.indexSettings = indexSettings;
        // TODO: should the logger be updated to reflect the renamed shard?
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    @Override
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public IndexSettings indexSettings() {
        return indexSettings;
    }

    public void renameTo(Index index) {
        synchronized (this) {
            final var oldShardId = this.shardId;
            this.shardId = new ShardId(index, this.shardId.getId());
            logger.info("Renamed shard from [{}] to [{}]", oldShardId, index);
        }
    }
}
