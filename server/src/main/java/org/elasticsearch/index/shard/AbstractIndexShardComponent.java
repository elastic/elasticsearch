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

    protected volatile Logger logger;
    protected volatile ShardId shardId;
    protected final IndexSettings indexSettings;

    protected AbstractIndexShardComponent(ShardId shardId, IndexSettings indexSettings) {
        this.shardId = shardId;
        this.indexSettings = indexSettings;
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

    /**
     * Points this component at the renamed index. Only the name changes: {@code index} must have the same UUID as the current
     * shard id's index, which is why {@link #shardId} is volatile rather than final. The logger is re-created so that its prefix
     * carries the new shard id: a logger's prefix cannot be changed in place, which is why {@link #logger} is volatile too. A
     * subclass that keeps other state in step with the shard id (such as {@link IndexShard} and its routing entry) overrides this
     * and takes its own lock around the whole update.
     */
    public void renameTo(Index index) {
        assert index.getUUID().equals(shardId.getIndex().getUUID()) : "rename must keep the index uuid: " + shardId + " -> " + index;
        final var oldShardId = this.shardId;
        final var newShardId = new ShardId(index, oldShardId.getId());
        this.shardId = newShardId;
        this.logger = Loggers.getLogger(getClass(), newShardId);
        logger.debug("renamed shard from [{}] to [{}]", oldShardId, newShardId);
    }
}
