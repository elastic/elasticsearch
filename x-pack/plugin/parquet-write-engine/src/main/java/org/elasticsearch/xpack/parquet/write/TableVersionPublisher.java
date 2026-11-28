/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.index.shard.ShardId;

/**
 * Tells the search replicas of a shard that its primary has committed a new version of the table. A primary of the write engine knows
 * when its uploads become part of the table (a flush, or a compaction that replaced files), so its replicas read the table then, and do
 * not have to wait for their scheduled refresh. This is the role that the notification of a new commit has in stateless, and it reuses
 * the refresh of the unpromotable copies that {@code _refresh} sends.
 *
 * <p>An announcement is best effort: one that is lost (a replica that is starting, a disconnection) costs only time, because each
 * replica still reads the table on its scheduled refresh.
 */
@FunctionalInterface
public interface TableVersionPublisher {

    TableVersionPublisher NONE = (shardId, primaryTerm, version) -> {};

    /**
     * Announces a version of the table. It must not block: it is called on the thread of the flush or of the compaction.
     *
     * @param version the version of the table that the primary committed, which lists every upload of the shard so far
     */
    void publish(ShardId shardId, long primaryTerm, long version);
}
