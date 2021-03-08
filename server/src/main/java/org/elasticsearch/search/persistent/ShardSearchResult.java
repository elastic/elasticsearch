/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;

import java.io.IOException;

public class ShardSearchResult implements Writeable {
    private final String id;
    private final String persistentSearchId;
    private final int shardIndex;
    private final long expirationTime;
    private final QueryFetchSearchResult result;

    public ShardSearchResult(String id, String persistentSearchId, int shardIndex, long expirationTime, QueryFetchSearchResult result) {
        this.id = id;
        this.persistentSearchId = persistentSearchId;
        this.shardIndex = shardIndex;
        this.expirationTime = expirationTime;
        this.result = result;
    }

    public ShardSearchResult(StreamInput in) throws IOException {
        this.id = in.readString();
        this.persistentSearchId = in.readString();
        this.shardIndex = in.readInt();
        this.expirationTime = in.readLong();
        this.result = new QueryFetchSearchResult(in);
    }

    public String getPersistentSearchId() {
        return persistentSearchId;
    }

    public String getId() {
        return id;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public QueryFetchSearchResult getResult() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(persistentSearchId);
        out.writeInt(shardIndex);
        out.writeLong(expirationTime);
        result.writeTo(out);
    }
}
