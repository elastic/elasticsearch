/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.index.shard.ShardId;

/**
 * Provides information about state of commit uploads of a shard.
 */
public interface ShardCommitUploadStats {
    ShardId shardId();

    long pendingUploadBytes();
}
