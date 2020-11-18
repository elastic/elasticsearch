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

package org.elasticsearch.action.termenum;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;

/**
 * Internal response of a term enum request executed directly against a specific shard.
 *
 *
 */
class ShardTermEnumResponse extends BroadcastShardResponse {

    private String error;
    private boolean timedOut;

    private List<TermCount> terms;

    ShardTermEnumResponse(StreamInput in) throws IOException {
        super(in);
        terms = in.readList(TermCount::new);
        error = in.readOptionalString();
        timedOut = in.readBoolean();
    }

    ShardTermEnumResponse(ShardId shardId, List<TermCount> terms, String error, boolean timedOut) {
        super(shardId);
        this.terms = terms;
        this.error = error;
        this.timedOut = timedOut;
    }

    public List<TermCount> terms() {
        return this.terms;
    }

    public String getError() {
        return error;
    }

    public boolean getTimedOut() {
        return timedOut;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(terms);
        out.writeOptionalString(error);
        out.writeBoolean(timedOut);
    }
}
