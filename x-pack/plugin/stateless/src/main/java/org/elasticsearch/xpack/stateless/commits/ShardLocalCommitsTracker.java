/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

/**
 * Encapsulates shard-level tracking of acquired commits and open readers to prevent
 * premature deletion of referenced blobs during engine operations.
 *
 * <p>
 *  This tracker maintains state that persists across {@link org.elasticsearch.index.shard.IndexShard#resetEngine}
 *  operations, ensuring that commit references and reader state are preserved to avoid
 *  deleting blobs that are still in use by active readers or referenced commits.
 * </p>
 */
public record ShardLocalCommitsTracker(ShardLocalReadersTracker shardLocalReadersTracker, ShardLocalCommitsRefs shardLocalCommitsRefs) {}
