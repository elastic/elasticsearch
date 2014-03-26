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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal terms by query response of a shard terms by query request executed directly against a specific shard.
 */
class ShardTermsByQueryResponse extends BroadcastShardOperationResponse {

    private ResponseTerms responseTerms;

    /**
     * Default constructor
     */
    ShardTermsByQueryResponse() {
    }

    /**
     * Main constructor
     *
     * @param index         the index the request executed against
     * @param shardId       the id of the shard the request executed on
     * @param responseTerms the terms gathered from the shard
     */
    public ShardTermsByQueryResponse(String index, int shardId, ResponseTerms responseTerms) {
        super(index, shardId);
        this.responseTerms = responseTerms;
    }

    /**
     * Gets the gathered terms.
     *
     * @return the {@link ResponseTerms}
     */
    public ResponseTerms getResponseTerms() {
        return this.responseTerms;
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
        responseTerms = ResponseTerms.deserialize(in);
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
        ResponseTerms.serialize(responseTerms, out);
    }
}
