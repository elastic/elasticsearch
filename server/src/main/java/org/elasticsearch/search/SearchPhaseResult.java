/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * This class is a base class for all search related results. It contains the shard target it
 * was executed against, a shard index used to reference the result on the coordinating node
 * and a request ID that is used to reference the request context on the executing node. The
 * request ID is particularly important since it is used to reference and maintain a context
 * across search phases to ensure the same point in time snapshot is used for querying and
 * fetching etc.
 */
public abstract class SearchPhaseResult extends TransportResponse {

    private SearchShardTarget searchShardTarget;
    private int shardIndex = -1;
    protected long requestId;
    @Nullable
    private final SetOnce<TaskInfo> taskInfo = new SetOnce<>();

    protected SearchPhaseResult() {
    }

    protected SearchPhaseResult(StreamInput in) throws IOException {
        super(in);
        //TODO update version when backporting
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.taskInfo.set(in.readOptionalWriteable(TaskInfo::new));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        //TODO update version when backporting
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeOptionalWriteable(this.taskInfo.get());
        }
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo.set(taskInfo);
    }

    public TaskInfo getTaskInfo() {
        return this.taskInfo.get();
    }

    /**
     * Returns the results request ID that is used to reference the search context on the executing node
     */
    public long getRequestId() {
        return requestId;
    }

    /**
     * Returns the shard index in the context of the currently executing search request that is
     * used for accounting on the coordinating node
     */
    public int getShardIndex() {
        assert shardIndex != -1 : "shardIndex is not set";
        return shardIndex;
    }

    public SearchShardTarget getSearchShardTarget() {
        return searchShardTarget;
    }

    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        this.searchShardTarget = shardTarget;
    }

    public void setShardIndex(int shardIndex) {
        assert shardIndex >= 0 : "shardIndex must be >= 0 but was: " + shardIndex;
        this.shardIndex = shardIndex;
    }

    /**
     * Returns the query result iff it's included in this response otherwise <code>null</code>
     */
    public QuerySearchResult queryResult() {
        return null;
    }

    /**
     * Returns the fetch result iff it's included in this response otherwise <code>null</code>
     */
    public FetchSearchResult fetchResult() { return null; }
}
