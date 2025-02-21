/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.autoshard;

import org.elasticsearch.action.support.ActiveShardCount;

/**
 * Cluster state update request that allows re-sharding an index
 * At the moment, we only have the ability to increment the number of shards
 * of an index (by a multiplicative factor).
 * We do not support removing shards from an index.
 */
public class AutoshardIndexClusterStateUpdateRequest {
    private final String cause;
    private final String index;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public AutoshardIndexClusterStateUpdateRequest(String cause, String index) {
        this.cause = cause;
        this.index = index;
    }

    public AutoshardIndexClusterStateUpdateRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public String cause() {
        return cause;
    }

    public String index() {
        return index;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    @Override
    public String toString() {
        return "AutoshardIndexClusterStateUpdateRequest{"
            + "cause='"
            + cause
            + '\''
            + ", index='"
            + index
            + '\''
            + ", waitForActiveShards="
            + waitForActiveShards
            + '}';
    }
}
