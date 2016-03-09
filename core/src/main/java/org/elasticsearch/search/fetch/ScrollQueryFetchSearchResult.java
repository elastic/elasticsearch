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

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

import static org.elasticsearch.search.fetch.QueryFetchSearchResult.readQueryFetchSearchResult;

/**
 *
 */
public class ScrollQueryFetchSearchResult extends TransportResponse {

    private QueryFetchSearchResult result;
    private SearchShardTarget shardTarget;

    public ScrollQueryFetchSearchResult() {
    }

    public ScrollQueryFetchSearchResult(QueryFetchSearchResult result, SearchShardTarget shardTarget) {
        this.result = result;
        this.shardTarget = shardTarget;
    }

    public QueryFetchSearchResult result() {
        return result;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardTarget = new SearchShardTarget(in);
        result = readQueryFetchSearchResult(in);
        result.shardTarget(shardTarget);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardTarget.writeTo(out);
        result.writeTo(out);
    }
}
