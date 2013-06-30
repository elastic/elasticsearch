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

package org.elasticsearch.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal terms by query request executed directly against a specific index shard.
 */
class ShardTermsByQueryRequest extends BroadcastShardOperationRequest {

    @Nullable
    private String[] filteringAliases;
    private TermsByQueryRequest request;

    /**
     * Default constructor
     */
    ShardTermsByQueryRequest() {
    }

    /**
     * Main Constructor
     *
     * @param index            the index of the shard request
     * @param shardId          the id of the shard the request is for
     * @param filteringAliases optional aliases
     * @param request          the original {@link TermsByQueryRequest}
     */
    public ShardTermsByQueryRequest(String index, int shardId, @Nullable String[] filteringAliases, TermsByQueryRequest request) {
        super(index, shardId, request);
        this.filteringAliases = filteringAliases;
        this.request = request;
    }

    /**
     * Gets the filtering aliases
     *
     * @return the filtering aliases
     */
    public String[] filteringAliases() {
        return filteringAliases;
    }

    /**
     * Gets the original {@link TermsByQueryRequest}
     *
     * @return the request
     */
    public TermsByQueryRequest request() {
        return request;
    }

    /**
     * Deserialize
     *
     * @param in the input
     * @throws IOException
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            request = new TermsByQueryRequest();
            request.readFrom(in);
        }

        if (in.readBoolean()) {
            filteringAliases = in.readStringArray();
        }
    }

    /**
     * Serialize
     *
     * @param out the output
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (request == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            request.writeTo(out);
        }

        if (filteringAliases == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(filteringAliases);
        }
    }
}
