/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

public class QuerySearchRequest extends TransportRequest implements IndicesRequest {

    private final ShardSearchContextId contextId;
    private final AggregatedDfs dfs;
    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;

    public QuerySearchRequest(OriginalIndices originalIndices, ShardSearchContextId contextId,
                              ShardSearchRequest shardSearchRequest, AggregatedDfs dfs) {
        this.contextId = contextId;
        this.dfs = dfs;
        this.shardSearchRequest = shardSearchRequest;
        this.originalIndices = originalIndices;
    }

    public QuerySearchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        dfs = new AggregatedDfs(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            this.shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
        } else {
            this.shardSearchRequest = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        dfs.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(shardSearchRequest);
        }
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public AggregatedDfs dfs() {
        return dfs;
    }

    @Nullable
    public ShardSearchRequest shardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("id[");
        sb.append(contextId);
        sb.append("], ");
        sb.append("indices[");
        Strings.arrayToDelimitedString(originalIndices.indices(), ",", sb);
        sb.append("]");
        return sb.toString();
    }

}
