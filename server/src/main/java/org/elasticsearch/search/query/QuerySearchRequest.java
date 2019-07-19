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

package org.elasticsearch.search.query;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

public class QuerySearchRequest extends TransportRequest implements IndicesRequest {

    private long id;

    private AggregatedDfs dfs;

    private OriginalIndices originalIndices;

    public QuerySearchRequest() {
    }

    public QuerySearchRequest(OriginalIndices originalIndices, long id, AggregatedDfs dfs) {
        this.id = id;
        this.dfs = dfs;
        this.originalIndices = originalIndices;
    }

    public QuerySearchRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readLong();
        dfs = new AggregatedDfs(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        dfs.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    public long id() {
        return id;
    }

    public AggregatedDfs dfs() {
        return dfs;
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
        return new SearchTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("id[");
        sb.append(id);
        sb.append("], ");
        sb.append("indices[");
        Strings.arrayToDelimitedString(originalIndices.indices(), ",", sb);
        sb.append("]");
        return sb.toString();
    }

}
