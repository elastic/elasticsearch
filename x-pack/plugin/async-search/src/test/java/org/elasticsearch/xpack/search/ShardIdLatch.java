/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.CountDownLatch;

class ShardIdLatch extends CountDownLatch {
    private final ShardId shard;
    private final boolean shouldFail;

    ShardIdLatch(ShardId shard, boolean shouldFail) {
        super(1);
        this.shard = shard;
        this.shouldFail = shouldFail;
    }

    ShardId shardId() {
        return shard;
    }

    boolean shouldFail() {
        return shouldFail;
    }
}
