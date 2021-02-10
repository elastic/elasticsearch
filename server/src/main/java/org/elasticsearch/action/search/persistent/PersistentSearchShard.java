/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchRequest;

public class PersistentSearchShard implements Comparable<PersistentSearchShard> {
    private final SearchShard searchShard;
    private final ShardSearchRequest shardSearchRequest;
    private volatile boolean canBeSkipped;

    public PersistentSearchShard(SearchShard searchShard, ShardSearchRequest shardSearchRequest, boolean canBeSkipped) {
        this.searchShard = searchShard;
        this.shardSearchRequest = shardSearchRequest;
        this.canBeSkipped = canBeSkipped;
    }

    public boolean canBeSkipped() {
        return canBeSkipped;
    }

    public void setCanBeSkipped(boolean canBeSkipped) {
        this.canBeSkipped = canBeSkipped;
    }

    public SearchShard getSearchShard() {
        return searchShard;
    }

    @Override
    public int compareTo(PersistentSearchShard o) {
        Index index = getShardId().getIndex();
        Index otherIndex = o.getShardId().getIndex();
        int cmp = index.getName().compareTo(otherIndex.getName());
        return cmp != 0 ? cmp : index.getUUID().compareTo(otherIndex.getUUID());
    }

    public ShardId getShardId() {
        return searchShard.getShardId();
    }

    public ShardSearchRequest getRequest() {
        return shardSearchRequest;
    }
}
