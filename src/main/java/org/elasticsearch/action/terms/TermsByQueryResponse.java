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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * The response of the terms by query action.
 */
public class TermsByQueryResponse extends BroadcastOperationResponse {

    private ResponseTerms responseTerms;

    /**
     * Default constructor
     */
    TermsByQueryResponse() {
    }

    /**
     * Main constructor
     *
     * @param responseTerms    the merged terms
     * @param totalShards      the number of shards the request executed on
     * @param successfulShards the number of shards the request executed on successfully
     * @param failedShards     the number of failed shards
     * @param shardFailures    the failures
     */
    TermsByQueryResponse(ResponseTerms responseTerms, int totalShards, int successfulShards, int failedShards,
                         List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.responseTerms = responseTerms;
    }

    /**
     * Gets the merged terms
     *
     * @return the terms
     */
    public ResponseTerms getResponseTerms() {
        return responseTerms;
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
            responseTerms = ResponseTerms.deserialize(in);
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
        if (responseTerms == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ResponseTerms.serialize(responseTerms, out);
        }
    }
}
