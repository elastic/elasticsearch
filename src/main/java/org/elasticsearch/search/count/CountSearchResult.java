/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.count;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
 * The initial scan search result, including the count of hits matching the provided query.
 */
public class CountSearchResult implements Streamable, SearchPhaseResult {

    private long id;

    private long totalHits;

    private SearchShardTarget shardTarget;

    public CountSearchResult() {
    }

    public CountSearchResult(long id, long totalHits) {
        this.id = id;
        this.totalHits = totalHits;
    }

    public long id() {
        return this.id;
    }

    public long getTotalHits() {
        return this.totalHits;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readLong();
        totalHits = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        out.writeVLong(totalHits);
    }
}