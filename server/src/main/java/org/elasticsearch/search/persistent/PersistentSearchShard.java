/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class PersistentSearchShard implements Writeable, Comparable<PersistentSearchShard> {
    private final String id;
    private final String searchId;
    private final SearchShard searchShard;
    private volatile boolean canBeSkipped = false;

    public PersistentSearchShard(String id, String searchId, SearchShard searchShard) {
        this.id = id;
        this.searchId = searchId;
        this.searchShard = searchShard;
    }

    public PersistentSearchShard(StreamInput in) throws IOException {
        this.id = in.readString();
        this.searchId = in.readString();
        this.searchShard = new SearchShard(in);
        this.canBeSkipped = in.readBoolean();
    }

    public SearchShard getSearchShard() {
        return searchShard;
    }

    public ShardId getShardId() {
        return searchShard.getShardId();
    }

    public String getSearchId() {
        return searchId;
    }

    public String getId() {
        return id;
    }

    public boolean canBeSkipped() {
        return canBeSkipped;
    }

    public void setCanBeSkipped(boolean canBeSkipped) {
        this.canBeSkipped = canBeSkipped;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(searchId);
        searchShard.writeTo(out);
        out.writeBoolean(canBeSkipped);
    }

    @Override
    public int compareTo(PersistentSearchShard o) {
        return searchShard.compareTo(o.searchShard);
    }
}
