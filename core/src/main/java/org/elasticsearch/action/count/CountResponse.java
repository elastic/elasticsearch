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

package org.elasticsearch.action.count;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;

/**
 * The response of the count action.
 */
public class CountResponse extends BroadcastResponse {

    private final boolean terminatedEarly;
    private final long count;

    public CountResponse(SearchResponse searchResponse) {
        super(searchResponse.getTotalShards(), searchResponse.getSuccessfulShards(), searchResponse.getFailedShards(), Arrays.asList(searchResponse.getShardFailures()));
        this.count = searchResponse.getHits().totalHits();
        this.terminatedEarly = searchResponse.isTerminatedEarly() != null && searchResponse.isTerminatedEarly();
    }

    /**
     * The count of documents matching the query provided.
     */
    public long getCount() {
        return count;
    }

    /**
     * True if the request has been terminated early due to enough count
     */
    public boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public RestStatus status() {
        return RestStatus.status(getSuccessfulShards(), getTotalShards(), getShardFailures());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("CountResponse doesn't support being sent over the wire, just a shortcut to the search api");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CountResponse doesn't support being sent over the wire, just a shortcut to the search api");
    }
}
