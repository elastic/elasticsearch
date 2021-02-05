/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class ExecutePersistentQueryFetchRequest extends ActionRequest {
    private final PersistentSearchShardId searchShardId;
    private final long expireTime;
    private final ShardSearchRequest shardSearchRequest;

    public ExecutePersistentQueryFetchRequest(PersistentSearchShardId searchShardId,
                                              long expireTime,
                                              ShardSearchRequest shardSearchRequest) {
        this.searchShardId = searchShardId;
        this.expireTime = expireTime;
        this.shardSearchRequest = shardSearchRequest;
    }

    public ExecutePersistentQueryFetchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchShardId = new PersistentSearchShardId(in);
        this.expireTime = in.readLong();
        this.shardSearchRequest = new ShardSearchRequest(in);
    }

    public String getResultDocId() {
        return searchShardId.getDocId();
    }

    public String getSearchId() {
        return searchShardId.getSearchId();
    }

    public long getExpireTime() {
        return expireTime;
    }

    public int getShardIndex() {
        return searchShardId.getShardIndex();
    }

    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchShardId.writeTo(out);
        out.writeLong(expireTime);
        shardSearchRequest.writeTo(out);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return shardSearchRequest.createTask(id, type, action, parentTaskId, headers);
    }
}
