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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal validate response of a shard validate request executed directly against a specific shard.
 *
 *
 */
class ShardValidateQueryResponse extends BroadcastShardOperationResponse {

    private boolean valid;
    
    private String explanation;

    private String error;
    
    ShardValidateQueryResponse() {

    }

    public ShardValidateQueryResponse(String index, int shardId, boolean valid, String explanation, String error) {
        super(index, shardId);
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public boolean valid() {
        return this.valid;
    }
    
    public String explanation() {
        return explanation;
    }
    
    public String error() {
        return error;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        valid = in.readBoolean();
        explanation = in.readOptionalUTF();
        error = in.readOptionalUTF();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeOptionalUTF(explanation);
        out.writeOptionalUTF(error);
    }
}
