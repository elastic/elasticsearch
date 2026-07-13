/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.index.shard.ShardId;

public record FileCacheKey(ShardId shardId, long primaryTerm, String fileName) implements SharedBlobCacheService.KeyBase {
    public FileCacheKey {
        assert shardId != null;
    }
}
