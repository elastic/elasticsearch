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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.persistent.ShardQueryResultInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ReducePartialPersistentSearchRequest extends ActionRequest {
    private final String searchId;
    private final List<ShardQueryResultInfo> shardsToReduce;
    private final SearchRequest originalRequest;
    private final long searchAbsoluteStartMillis;
    private final long searchRelativeStartNanos;
    private final long expirationTime;
    private final int reductionRound;
    private final boolean performFinalReduce;

    public ReducePartialPersistentSearchRequest(String searchId,
                                                List<ShardQueryResultInfo> shardsToReduce,
                                                SearchRequest originalRequest,
                                                long searchAbsoluteStartMillis,
                                                long searchRelativeStartNanos,
                                                long expirationTime,
                                                int reductionRound,
                                                boolean performFinalReduce) {
        this.searchId = searchId;
        this.shardsToReduce = List.copyOf(shardsToReduce);
        this.originalRequest = originalRequest;
        this.searchAbsoluteStartMillis = searchAbsoluteStartMillis;
        this.searchRelativeStartNanos = searchRelativeStartNanos;
        this.expirationTime = expirationTime;
        this.reductionRound = reductionRound;
        this.performFinalReduce = performFinalReduce;
    }

    public ReducePartialPersistentSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchId = in.readString();
        this.shardsToReduce = in.readList(ShardQueryResultInfo::new);
        this.originalRequest = new SearchRequest(in);
        this.searchAbsoluteStartMillis = in.readLong();
        this.searchRelativeStartNanos = in.readLong();
        this.expirationTime = in.readLong();
        this.reductionRound = in.readInt();
        this.performFinalReduce = in.readBoolean();
    }

    public String getSearchId() {
        return searchId;
    }

    public List<ShardQueryResultInfo> getShardsToReduce() {
        return shardsToReduce;
    }

    public SearchRequest getOriginalRequest() {
        return originalRequest;
    }

    public long getSearchAbsoluteStartMillis() {
        return searchAbsoluteStartMillis;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public long getSearchRelativeStartNanos() {
        return searchRelativeStartNanos;
    }

    public int getReductionRound() {
        return reductionRound;
    }

    public boolean performFinalReduce() {
        return performFinalReduce;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(searchId);
        out.writeList(shardsToReduce);
        originalRequest.writeTo(out);
        out.writeLong(searchAbsoluteStartMillis);
        out.writeLong(searchRelativeStartNanos);
        out.writeLong(expirationTime);
        out.writeInt(reductionRound);
        out.writeBoolean(performFinalReduce);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::getDescription, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
